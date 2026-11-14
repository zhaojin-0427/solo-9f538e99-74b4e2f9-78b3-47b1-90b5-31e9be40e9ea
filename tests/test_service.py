"""端到端与存储层测试。

运行：
    python -m pytest tests/ -v
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import app
from app.storage import (
    PartitionStore,
    RejectedUpload,
    UploadTooLarge,
    validate_day,
)

DAY = "2026-11-14"
OTHER_DAY = "2026-11-13"


def make_line(event_id: str, day: str = DAY, hour: int = 10, chute: str = "A-01") -> bytes:
    event = {
        "event_id": event_id,
        "scanned_at": f"{day}T{hour:02d}:00:00Z",
        "chute": chute,
    }
    return (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")


def make_batch(prefix: str, n: int, day: str = DAY) -> bytes:
    return b"".join(make_line(f"{prefix}-{i:05d}", day=day, hour=8 + i % 12) for i in range(n))


def as_lines(body: bytes) -> list[bytes]:
    """把整批 NDJSON 拆成逐行字节，供存储层上传接口消费。"""
    return body.splitlines(keepends=True)


@pytest.fixture()
def data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture()
def store(data_dir: Path) -> PartitionStore:
    config = Settings(data_dir=str(data_dir), max_upload_bytes=10 * 1024 * 1024,
                      reap_wait_seconds=5)
    s = PartitionStore(data_dir, config=config)
    yield s
    s.close()


@pytest.fixture()
def client(data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("MAX_UPLOAD_BYTES", str(10 * 1024 * 1024))
    monkeypatch.setenv("REAP_WAIT_SECONDS", "5")
    with TestClient(app) as c:
        yield c


def read_all_events(client: TestClient, day: str) -> list[dict]:
    resp = client.get(f"/partitions/{day}/events")
    assert resp.status_code == 200
    return [json.loads(line) for line in resp.content.splitlines() if line.strip()]


# --------------------------------------------------------------- 基础上传/拒收
class TestUpload:
    def test_valid_upload_published(self, client: TestClient) -> None:
        body = make_batch("ev", 5)
        resp = client.post(f"/partitions/{DAY}/logs", content=body,
                           headers={"Content-Type": "application/x-ndjson"})
        assert resp.status_code == 201
        data = resp.json()
        assert data["event_count"] == 5
        events = read_all_events(client, DAY)
        assert len(events) == 5
        assert events[0]["event_id"] == "ev-00000"

    def test_foreign_date_rejects_whole_file(self, client: TestClient) -> None:
        body = make_batch("ok", 3) + make_line("bad", day=OTHER_DAY) + make_batch("ok2", 2)
        resp = client.post(f"/partitions/{DAY}/logs", content=body)
        assert resp.status_code == 422
        assert "整份拒收" in resp.json()["detail"]
        # 整份拒收：分区内查不到任何事件
        assert read_all_events(client, DAY) == []
        stats = client.get(f"/partitions/{DAY}/stats").json()
        assert stats["published_files"] == 0
        assert stats["total_events"] == 0

    def test_malformed_json_rejected(self, client: TestClient) -> None:
        body = make_batch("ok", 2) + b"{not json\n"
        resp = client.post(f"/partitions/{DAY}/logs", content=body)
        assert resp.status_code == 422
        assert read_all_events(client, DAY) == []

    def test_missing_field_rejected(self, client: TestClient) -> None:
        bad = json.dumps({"event_id": "x", "scanned_at": f"{DAY}T10:00:00Z"}) + "\n"
        resp = client.post(f"/partitions/{DAY}/logs", content=bad.encode())
        assert resp.status_code == 422
        assert "chute" in resp.json()["detail"]

    def test_non_utc_timezone_rejected(self, client: TestClient) -> None:
        event = {"event_id": "x", "scanned_at": f"{DAY}T10:00:00+08:00", "chute": "A-1"}
        resp = client.post(f"/partitions/{DAY}/logs",
                           content=(json.dumps(event) + "\n").encode())
        assert resp.status_code == 422

    def test_empty_and_blank_line_rejected(self, client: TestClient) -> None:
        resp = client.post(f"/partitions/{DAY}/logs", content=b"\n")
        assert resp.status_code == 422
        resp = client.post(f"/partitions/{DAY}/logs", content=b"   \n")
        assert resp.status_code == 422

    def test_missing_trailing_newline_rejected(self, client: TestClient) -> None:
        body = make_batch("ok", 2).rstrip(b"\n")
        resp = client.post(f"/partitions/{DAY}/logs", content=body)
        assert resp.status_code == 422

    def test_size_limit_enforced(self, data_dir: Path) -> None:
        config = Settings(data_dir=str(data_dir), max_upload_bytes=256,
                          reap_wait_seconds=5)
        small_store = PartitionStore(data_dir, config=config)
        try:
            with pytest.raises(UploadTooLarge):
                small_store.upload(DAY, iter(as_lines(make_batch("big", 20))))
        finally:
            small_store.close()
        # 超限后没有任何文件发布
        fresh = PartitionStore(data_dir, config=config)
        try:
            assert fresh.stats(DAY)["published_files"] == 0
        finally:
            fresh.close()

    def test_bad_day_format(self, client: TestClient) -> None:
        resp = client.post("/partitions/2026-13-40/logs", content=b"")
        assert resp.status_code == 400


# --------------------------------------------------------------- 压实语义
class TestCompaction:
    def test_compaction_preserves_event_multiset(self, client: TestClient) -> None:
        # 同一事件 ID 可重复出现，压实不做去重
        bodies = [
            make_batch("a", 7) + make_line("dup", hour=9),
            make_batch("b", 11),
            make_batch("c", 4) + make_line("dup", hour=11),
        ]
        for body in bodies:
            assert client.post(f"/partitions/{DAY}/logs", content=body).status_code == 201

        before = Counter(json.dumps(e, sort_keys=True) for e in read_all_events(client, DAY))
        stats_before = client.get(f"/partitions/{DAY}/stats").json()
        assert stats_before["published_files"] == 3
        assert stats_before["total_events"] == len(before)

        result = client.post(f"/partitions/{DAY}/compact").json()
        assert result["compacted"] is True
        assert result["files_before"] == 3
        assert result["events"] == len(before)

        stats_after = client.get(f"/partitions/{DAY}/stats").json()
        assert stats_after["published_files"] == 1
        assert stats_after["total_events"] == len(before)

        after = Counter(json.dumps(e, sort_keys=True) for e in read_all_events(client, DAY))
        assert before == after  # 各事件出现次数完全不变
        # 重复事件仍保留两份
        dup_events = [e for e in read_all_events(client, DAY) if e["event_id"] == "dup"]
        assert len(dup_events) == 2

    def test_compact_then_new_uploads_compact_again(self, client: TestClient) -> None:
        client.post(f"/partitions/{DAY}/logs", content=make_batch("a", 5))
        client.post(f"/partitions/{DAY}/compact")
        client.post(f"/partitions/{DAY}/logs", content=make_batch("b", 3))
        stats = client.get(f"/partitions/{DAY}/stats").json()
        assert stats["published_files"] == 2
        client.post(f"/partitions/{DAY}/compact")
        stats = client.get(f"/partitions/{DAY}/stats").json()
        assert stats["published_files"] == 1
        events = read_all_events(client, DAY)
        assert len(events) == 8

    def test_compact_empty_partition(self, client: TestClient) -> None:
        result = client.post(f"/partitions/{DAY}/compact").json()
        assert result["compacted"] is False
        # 空分区读取返回 200 和空 NDJSON
        resp = client.get(f"/partitions/{DAY}/events")
        assert resp.status_code == 200
        assert resp.content == b""

    def test_idempotent_compaction_with_single_file(self, client: TestClient) -> None:
        client.post(f"/partitions/{DAY}/logs", content=make_batch("a", 4))
        result = client.post(f"/partitions/{DAY}/compact").json()
        assert result["compacted"] is True
        assert result["files_before"] == 1
        events = read_all_events(client, DAY)
        assert len(events) == 4


# --------------------------------------------------------------- 并发安全
class TestConcurrency:
    def test_concurrent_uploads_none_lost(self, store: PartitionStore) -> None:
        n_threads = 8
        per_thread = 50
        errors: list[Exception] = []

        def worker(tid: int) -> None:
            try:
                lines = [make_line(f"t{tid}-{i:04d}", hour=8 + i % 12)
                         for i in range(per_thread)]
                store.upload(DAY, iter(lines))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        stats = store.stats(DAY)
        assert stats["published_files"] == n_threads
        assert stats["total_events"] == n_threads * per_thread

        with store.read_session(DAY) as session:
            count = len(b"".join(session.iter_lines()).splitlines())
        assert count == n_threads * per_thread

    def test_compact_concurrent_with_uploads(self, store: PartitionStore) -> None:
        # 先发布一批
        for i in range(3):
            store.upload(DAY, iter([make_line(f"base-{i}-{j:03d}") for j in range(20)]))

        uploaded_after = {"n": 0}
        upload_errors: list[Exception] = []

        def compactor() -> None:
            # 让压实与上传充分交错
            time.sleep(0.05)
            store.compact(DAY)

        def uploader(tid: int) -> None:
            try:
                lines = [make_line(f"late-{tid}-{j:03d}", hour=20) for j in range(25)]
                store.upload(DAY, iter(lines))
                uploaded_after["n"] += len(lines)
            except Exception as exc:  # noqa: BLE001
                upload_errors.append(exc)

        threads = [threading.Thread(target=compactor)]
        for tid in range(4):
            threads.append(threading.Thread(target=uploader, args=(tid,)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert upload_errors == []
        stats = store.stats(DAY)
        # 60 条基础 + 100 条压实期间/之后上传，一条都不能丢
        assert stats["total_events"] == 160
        with store.read_session(DAY) as session:
            lines = b"".join(session.iter_lines()).splitlines()
        assert len(lines) == 160
        ids = {json.loads(line)["event_id"] for line in lines}
        assert len(ids) == 160  # 全部 ID 唯一，说明无丢失无重复

        # 再压一次应得到单文件且计数不变
        store.compact(DAY)
        assert store.stats(DAY)["published_files"] == 1
        with store.read_session(DAY) as session:
            assert len(b"".join(session.iter_lines()).splitlines()) == 160

    def test_reader_sees_consistent_snapshot_across_compaction(self, store: PartitionStore) -> None:
        store.upload(DAY, iter([make_line(f"s-{i:03d}") for i in range(30)]))
        seen_during: list[int] = []
        release = threading.Event()

        def slow_reader() -> None:
            with store.read_session(DAY) as session:
                chunks: list[bytes] = []
                for chunk in session.iter_lines():
                    chunks.append(chunk)
                    release.wait(timeout=5)
                seen_during.append(len(b"".join(chunks).splitlines()))

        reader = threading.Thread(target=slow_reader)
        reader.start()
        time.sleep(0.1)
        store.upload(DAY, iter([make_line(f"s2-{i:03d}", hour=21) for i in range(10)]))
        store.compact(DAY)
        # 压实完成后数据文件应只剩一份 published
        assert store.stats(DAY)["published_files"] == 1
        release.set()
        reader.join()
        assert seen_during[0] == 30  # 读者看到的始终是它打开时的完整分区状态

        # 新读者看到压实后的完整状态
        with store.read_session(DAY) as session:
            assert len(b"".join(session.iter_lines()).splitlines()) == 40

    def test_superseded_files_reaped_after_reads_finish(self, store: PartitionStore) -> None:
        store.upload(DAY, iter([make_line(f"r-{i:03d}") for i in range(10)]))
        store.upload(DAY, iter([make_line(f"r2-{i:03d}", hour=12) for i in range(10)]))
        before_files = {p.name for p in store.events_dir.glob("*.ndjson")}
        store.compact(DAY)
        time.sleep(0.2)
        after_files = {p.name for p in store.events_dir.glob("*.ndjson")}
        assert len(after_files) == 1
        assert before_files.isdisjoint(after_files)


# --------------------------------------------------------------- 故障与恢复
class TestFailureRecovery:
    def test_index_insert_failure_keeps_old_data_readable(self, store: PartitionStore,
                                                          monkeypatch: pytest.MonkeyPatch) -> None:
        # 先有一份健康数据
        store.upload(DAY, iter([make_line(f"good-{i:03d}") for i in range(6)]))

        real_publish = store._publish_file

        def broken_publish(partition_day: str, filename: str, digest: str,
                           event_count: int, size_bytes: int) -> None:
            raise sqlite3.OperationalError("simulated index failure")

        monkeypatch.setattr(store, "_publish_file", broken_publish)
        with pytest.raises(sqlite3.OperationalError):
            store.upload(DAY, iter([make_line(f"new-{i:03d}", hour=15) for i in range(3)]))
        monkeypatch.undo()
        assert real_publish is not broken_publish

        # 旧数据仍完整可读
        with store.read_session(DAY) as session:
            lines = b"".join(session.iter_lines()).splitlines()
        assert len(lines) == 6
        assert all(json.loads(line)["event_id"].startswith("good-") for line in lines)
        # 失败的数据文件被隔离而不是丢失
        quarantined = list(store.quarantine_dir.glob("*.rej"))
        assert len(quarantined) == 1
        assert b"new-000" in quarantined[0].read_bytes()

    def test_swap_failure_leaves_published_files_intact(self, store: PartitionStore,
                                                        monkeypatch: pytest.MonkeyPatch) -> None:
        store.upload(DAY, iter([make_line(f"k-{i:03d}") for i in range(5)]))
        store.upload(DAY, iter([make_line(f"k2-{i:03d}", hour=11) for i in range(5)]))

        def broken_swap(*args: object, **kwargs: object) -> None:
            raise sqlite3.OperationalError("simulated commit failure")

        monkeypatch.setattr(store, "_commit_compaction_swap", broken_swap)
        from app.storage import CompactionError
        with pytest.raises(CompactionError):
            store.compact(DAY)
        monkeypatch.undo()

        stats = store.stats(DAY)
        assert stats["published_files"] == 2
        assert stats["total_events"] == 10
        with store.read_session(DAY) as session:
            assert len(b"".join(session.iter_lines()).splitlines()) == 10

    def test_reopen_recovers_and_cleans_orphans(self, data_dir: Path) -> None:
        config = Settings(data_dir=str(data_dir), max_upload_bytes=10 * 1024 * 1024,
                          reap_wait_seconds=5)
        s1 = PartitionStore(data_dir, config=config)
        s1.upload(DAY, iter([make_line(f"p-{i:03d}") for i in range(7)]))
        s1.upload(DAY, iter([make_line(f"p2-{i:03d}", hour=13) for i in range(3)]))
        s1.compact(DAY)
        # 伪造一个崩溃残留的临时文件
        orphan = data_dir / "tmp" / "upload-deadbeef.ndjson"
        orphan.write_bytes(b'{"event_id":"ghost"}\n')
        # 模拟回收前崩溃：把一条 superseded 记录对应的数据文件重新造出来
        row = s1._reader.execute(
            "SELECT filename FROM files WHERE state='superseded' LIMIT 1"
        ).fetchone()
        leftover = data_dir / "events" / row["filename"]
        leftover.write_bytes(b'{"event_id":"leftover"}\n')
        assert leftover.exists()
        s1.close()

        s2 = PartitionStore(data_dir, config=config)
        try:
            assert not orphan.exists()
            assert not leftover.exists()
            with s2.read_session(DAY) as session:
                assert len(b"".join(session.iter_lines()).splitlines()) == 10
        finally:
            s2.close()


# --------------------------------------------------------------- 存储层杂项
class TestStorageUnit:
    def test_validate_day(self) -> None:
        assert validate_day("2026-11-14") == "2026-11-14"
        for bad in ("2026-1-1", "2026/11/14", "2026-13-01", "monday"):
            with pytest.raises(ValueError):
                validate_day(bad)

    def test_upload_bytes_preserved_exactly(self, store: PartitionStore) -> None:
        body = make_batch("z", 3)
        result = store.upload(DAY, iter(as_lines(body)))
        on_disk = (store.events_dir / result["filename"]).read_bytes()
        assert on_disk == body
        import hashlib
        assert result["sha256"] == hashlib.sha256(body).hexdigest()

    def test_rejected_upload_leaves_no_tmp_files(self, store: PartitionStore) -> None:
        bad = make_batch("ok", 2) + make_line("x", day=OTHER_DAY)
        with pytest.raises(RejectedUpload):
            store.upload(DAY, iter(as_lines(bad)))
        assert list(store.tmp_dir.glob("*")) == []

    def test_healthz(self, client: TestClient) -> None:
        resp = client.get("/healthz")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_utc_now_helper_unused_but_import_sanity(self) -> None:
        dt = datetime.now(UTC)
        assert dt.tzinfo is not None
