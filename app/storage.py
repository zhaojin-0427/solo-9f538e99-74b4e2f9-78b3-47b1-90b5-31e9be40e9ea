"""分区存储核心：SQLite 索引 + 本地 NDJSON 事件文件。

设计要点
========
* 每个分区（按 UTC 日期）由若干「已发布」NDJSON 文件组成，SQLite 保存文件索引。
* 上传：先把完整数据流式落到临时文件并逐行校验（混入其他日期即整份拒收），
  fsync 后再原子写入索引；索引失败时数据文件仍保留并转为隔离文件，旧数据不受影响。
* 读取：先在短锁内取出该分区当前「已发布」文件的一致快照，再流式回放，
  查询期间看到的始终是某个完整分区状态，不会读到半写文件或被压实中途切换。
* 压实：合并快照内的全部已发布文件 -> 临时文件 -> 双重校验（行数/sha256）->
  单事务原子切换索引 -> 旧文件等待在途查询结束后才回收。
* 任何一步失败，新文件/临时文件都会被清理，已发布的旧文件始终可读。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .config import Settings, settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    partition_day TEXT    NOT NULL,
    filename      TEXT    NOT NULL UNIQUE,
    sha256        TEXT    NOT NULL,
    event_count   INTEGER NOT NULL CHECK (event_count >= 0),
    size_bytes    INTEGER NOT NULL CHECK (size_bytes >= 0),
    state         TEXT    NOT NULL CHECK (state IN ('published', 'superseded')),
    created_at    TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    superseded_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_partition_state
    ON files (partition_day, state, id);
"""


class RejectedUpload(Exception):
    """上传内容未通过校验，整份拒收。"""

    def __init__(self, message: str, line_no: int | None = None) -> None:
        super().__init__(message)
        self.line_no = line_no


class UploadTooLarge(Exception):
    """上传体积超过配置上限。"""


class CompactionError(Exception):
    """压实过程中发生完整性错误，分区保持压实前状态。"""


def parse_utc_timestamp(raw: object) -> datetime:
    """解析 UTC 扫码时刻，要求显式带 Z 或 +00:00 的 ISO-8601。"""
    if not isinstance(raw, str):
        raise ValueError("scanned_at 必须是字符串")  # noqa: TRY004 - 统一由上层按校验失败处理
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"scanned_at 不是合法 ISO-8601 时间: {raw!r}") from exc
    if dt.tzinfo is None:
        raise ValueError("scanned_at 必须显式携带 UTC 时区（Z 或 +00:00）")
    if dt.utcoffset() != UTC.utcoffset(None):
        raise ValueError("scanned_at 必须使用 UTC 时区")
    return dt.astimezone(UTC)


def validate_day(day: str) -> str:
    """校验 YYYY-MM-DD 日期参数。"""
    try:
        parsed = datetime.strptime(day, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("日期必须形如 YYYY-MM-DD") from exc
    if parsed.strftime("%Y-%m-%d") != day:
        raise ValueError("日期必须形如 YYYY-MM-DD")
    return day


class PartitionStore:
    """文件索引与 NDJSON 数据文件的统一管理者（线程安全，单进程使用）。"""

    def __init__(self, data_dir: str | os.PathLike[str] | None = None,
                 config: Settings | None = None) -> None:
        self._settings = config or settings
        root = Path(data_dir or self._settings.data_dir)
        self.root = root
        self.events_dir = root / "events"
        self.tmp_dir = root / "tmp"
        self.quarantine_dir = root / "quarantine"
        for directory in (self.root, self.events_dir, self.tmp_dir, self.quarantine_dir):
            directory.mkdir(parents=True, exist_ok=True)

        self.db_path = root / "index.sqlite3"
        # check_same_thread=False：自行用锁保证访问串行化
        self._writer = sqlite3.connect(
            self.db_path, check_same_thread=False, isolation_level=None
        )
        self._writer.row_factory = sqlite3.Row
        self._writer.execute("PRAGMA journal_mode=WAL;")
        self._writer.execute("PRAGMA synchronous=FULL;")
        self._writer.execute("PRAGMA busy_timeout=5000;")
        self._reader = sqlite3.connect(
            self.db_path, check_same_thread=False, isolation_level=None
        )
        self._reader.row_factory = sqlite3.Row
        self._reader.execute("PRAGMA busy_timeout=5000;")

        self._index_lock = threading.Lock()   # 串行化所有索引写事务
        self._read_lock = threading.Lock()    # 串行化 reader 连接上的快照查询
        self._compact_lock = threading.Lock()  # 同一分区同时只允许一个压实任务

        # 正在进行中的读取会话 -> 会话引用的文件相对路径集合，用于安全回收旧文件
        self._active_reads: dict[int, set[str]] = {}
        self._read_counter = 0
        self._reads_lock = threading.Lock()

        self._writer.executescript(SCHEMA)
        self._startup_sweep()

    # ------------------------------------------------------------------ 工具
    def _event_path(self, filename: str) -> Path:
        return self.events_dir / filename

    def _new_tmp(self, prefix: str) -> Path:
        return self.tmp_dir / f"{prefix}-{uuid.uuid4().hex}.ndjson"

    def _quarantine(self, path: Path, reason: str) -> None:
        """把无法进入索引的文件移入隔离区，绝不静默删除用户数据。"""
        if not path.exists():
            return
        target = self.quarantine_dir / f"{path.name}.{uuid.uuid4().hex[:8]}.rej"
        try:
            os.replace(path, target)
            sidecar = target.with_suffix(target.suffix + ".reason")
            sidecar.write_text(reason + "\n", encoding="utf-8")
        except OSError:
            # 隔离也失败时保留原文件，等待人工处理
            pass

    def _startup_sweep(self) -> None:
        """进程启动时清理崩溃残留：临时文件删除，已废弃数据文件回收。"""
        for tmp in self.tmp_dir.glob("*.ndjson"):
            try:
                tmp.unlink()
            except OSError:
                pass
        with self._read_lock:
            rows = self._reader.execute(
                "SELECT filename FROM files WHERE state = 'superseded'"
            ).fetchall()
        for row in rows:
            self._safe_unlink(self._event_path(row["filename"]))

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        except OSError:
            # 某些文件系统不支持目录 fsync，忽略即可
            pass
        finally:
            os.close(fd)

    def _safe_unlink(self, path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass

    # ------------------------------------------------------------- 读取会话
    @contextmanager
    def read_session(self, partition_day: str) -> Iterator[ReadSession]:
        """取分区当前已发布文件的一致快照；会话结束前这些文件不会被回收。"""
        validate_day(partition_day)
        with self._read_lock:
            rows = self._reader.execute(
                "SELECT filename FROM files "
                "WHERE partition_day = ? AND state = 'published' ORDER BY id",
                (partition_day,),
            ).fetchall()
        files = tuple(row["filename"] for row in rows)
        with self._reads_lock:
            self._read_counter += 1
            session_id = self._read_counter
            self._active_reads[session_id] = set(files)
        try:
            yield ReadSession(self, partition_day, files)
        finally:
            with self._reads_lock:
                self._active_reads.pop(session_id, None)

    def _reap_superseded(self, filenames: Sequence[str]) -> None:
        """等待在途读取结束后回收旧文件；超时则留给下次压实/启动时回收。"""
        deadline = time.monotonic() + self._settings.reap_wait_seconds
        pending = set(filenames)
        while pending and time.monotonic() < deadline:
            with self._reads_lock:
                in_use = set().union(*self._active_reads.values()) if self._active_reads else set()
            remaining = {name for name in pending if name in in_use}
            for name in pending - in_use:
                self._safe_unlink(self._event_path(name))
            pending = remaining
            if pending:
                time.sleep(0.1)
        # 仍在使用的文件不强行删除，下次压实或进程重启时由 sweep 回收
        for name in list(pending):
            if not self._file_in_use(name):
                self._safe_unlink(self._event_path(name))
                pending.discard(name)

    def _file_in_use(self, filename: str) -> bool:
        with self._reads_lock:
            return any(filename in used for used in self._active_reads.values())

    # ------------------------------------------------- 索引写入接缝（便于故障注入测试）
    def _publish_file(self, partition_day: str, filename: str, digest: str,
                      event_count: int, size_bytes: int) -> None:
        """把一个新数据文件登记为 published（自动提交单条事务）。"""
        with self._index_lock:
            self._writer.execute(
                "INSERT INTO files (partition_day, filename, sha256, event_count, "
                "size_bytes, state) VALUES (?, ?, ?, ?, ?, 'published')",
                (partition_day, filename, digest, event_count, size_bytes),
            )

    def _commit_compaction_swap(self, partition_day: str, compacted_name: str, digest: str,
                                expected_events: int, size_bytes: int,
                                source_ids: Sequence[int]) -> None:
        """单事务原子完成压实索引切换：插入合并文件、旧文件置为 superseded。"""
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        with self._index_lock:
            self._writer.execute("BEGIN IMMEDIATE")
            try:
                # 并发上传可能在快照之后新增文件：只切换快照中的旧文件，
                # 新上传保持 published，绝不丢失。
                placeholders = ",".join("?" for _ in source_ids)
                confirmed = self._writer.execute(
                    f"SELECT id, event_count FROM files "
                    f"WHERE id IN ({placeholders}) AND state = 'published'",
                    source_ids,
                ).fetchall()
                if len(confirmed) != len(source_ids) or \
                        sum(row["event_count"] for row in confirmed) != expected_events:
                    raise CompactionError("压实期间索引状态变化，放弃切换")
                self._writer.execute(
                    "INSERT INTO files (partition_day, filename, sha256, event_count, "
                    "size_bytes, state) VALUES (?, ?, ?, ?, ?, 'published')",
                    (partition_day, compacted_name, digest, expected_events, size_bytes),
                )
                self._writer.execute(
                    f"UPDATE files SET state = 'superseded', superseded_at = ? "
                    f"WHERE id IN ({placeholders})",
                    [now, *source_ids],
                )
                self._writer.execute("COMMIT")
            except Exception:
                # 回滚失败不应掩盖导致回滚的原始错误
                try:
                    self._writer.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    # ------------------------------------------------------------------ 上传
    def upload(self, partition_day: str, line_iter: Iterator[bytes]) -> dict:
        """流式接收并校验一份日志，全部属于 partition_day 才发布，否则整份拒收。"""
        validate_day(partition_day)
        limit = self._settings.max_upload_bytes
        tmp = self._new_tmp("upload")
        hasher = hashlib.sha256()
        event_count = 0
        total_bytes = 0
        try:
            with open(tmp, "wb") as out:
                for line_no, raw_line in enumerate(line_iter, start=1):
                    if total_bytes + len(raw_line) > limit:
                        raise UploadTooLarge(
                            f"上传文件超过 {limit} 字节上限"
                        )
                    if not raw_line.endswith(b"\n"):
                        raise RejectedUpload("每行必须以换行符结尾", line_no)
                    payload = raw_line[:-1]
                    if not payload.strip():
                        raise RejectedUpload("不允许空行", line_no)
                    try:
                        event = json.loads(payload)
                    except json.JSONDecodeError as exc:
                        raise RejectedUpload(f"不是合法 JSON: {exc.msg}", line_no) from exc
                    self._validate_event(event, partition_day, line_no)
                    out.write(raw_line)
                    hasher.update(raw_line)
                    total_bytes += len(raw_line)
                    event_count += 1
                out.flush()
                os.fsync(out.fileno())
            self._fsync_dir(self.tmp_dir)

            if event_count == 0:
                raise RejectedUpload("文件为空或不含任何事件")

            filename = (
                f"{partition_day}-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:12]}.ndjson"
            )
            final_path = self._event_path(filename)
            os.replace(tmp, final_path)
            self._fsync_dir(self.events_dir)
        except (RejectedUpload, UploadTooLarge):
            self._safe_unlink(tmp)
            raise
        except Exception:
            # 落盘阶段的意外错误：清理临时文件，不动任何已发布数据
            self._safe_unlink(tmp)
            raise

        digest = hasher.hexdigest()
        try:
            self._publish_file(partition_day, filename, digest, event_count, total_bytes)
        except sqlite3.Error:
            # 索引切换失败：数据文件隔离保留，旧数据仍完整可读
            self._quarantine(final_path, f"index insert failed for partition {partition_day}")
            raise
        return {
            "partition_day": partition_day,
            "filename": filename,
            "event_count": event_count,
            "size_bytes": total_bytes,
            "sha256": digest,
        }

    @staticmethod
    def _validate_event(event: object, partition_day: str, line_no: int) -> None:
        if not isinstance(event, dict):
            raise RejectedUpload("每行必须是 JSON 对象", line_no)
        for field in ("event_id", "scanned_at", "chute"):
            if field not in event:
                raise RejectedUpload(f"缺少字段 {field}", line_no)
        event_id = event["event_id"]
        chute = event["chute"]
        if not isinstance(event_id, str) or not event_id.strip():
            raise RejectedUpload("event_id 必须是非空字符串", line_no)
        if not isinstance(chute, str) or not chute.strip():
            raise RejectedUpload("chute 必须是非空字符串", line_no)
        try:
            scanned = parse_utc_timestamp(event["scanned_at"])
        except ValueError as exc:
            raise RejectedUpload(str(exc), line_no) from exc
        if scanned.strftime("%Y-%m-%d") != partition_day:
            raise RejectedUpload(
                f"事件日期 {scanned.strftime('%Y-%m-%d')} 与上传分区 {partition_day} 不一致，整份拒收",
                line_no,
            )

    # ------------------------------------------------------------------ 压实
    def compact(self, partition_day: str) -> dict:
        """把该分区当前全部已发布文件合并为一份，事件出现次数保持不变。"""
        validate_day(partition_day)
        with self._compact_lock:
            with self._read_lock:
                rows = self._reader.execute(
                    "SELECT id, filename, event_count, sha256 FROM files "
                    "WHERE partition_day = ? AND state = 'published' ORDER BY id",
                    (partition_day,),
                ).fetchall()
            if not rows:
                return {
                    "partition_day": partition_day,
                    "compacted": False,
                    "reason": "分区不存在或没有已发布文件",
                    "files_before": 0,
                    "files_after": 0,
                    "events": 0,
                }

            sources = [(row["id"], row["filename"], row["event_count"], row["sha256"])
                       for row in rows]
            expected_events = sum(item[2] for item in sources)
            compacted_name = (
                f"{partition_day}-compacted-{datetime.now(UTC):%Y%m%dT%H%M%S}-"
                f"{uuid.uuid4().hex[:12]}.ndjson"
            )
            tmp = self._new_tmp("compact")
            hasher = hashlib.sha256()
            written = 0
            try:
                with open(tmp, "wb") as out:
                    for _id, filename, count, _digest in sources:
                        src = self._event_path(filename)
                        source_lines = 0
                        try:
                            with open(src, "rb") as inp:
                                for line in inp:
                                    if not line.endswith(b"\n"):
                                        raise CompactionError(
                                            f"源文件 {filename} 存在未以换行结尾的行"
                                        )
                                    out.write(line)
                                    hasher.update(line)
                                    written += 1
                                    source_lines += 1
                        except FileNotFoundError as exc:
                            raise CompactionError(
                                f"源文件 {filename} 缺失，压实中止"
                            ) from exc
                        if source_lines != count:
                            raise CompactionError(
                                f"源文件 {filename} 实际行数 {source_lines} 与索引记录 "
                                f"{count} 不一致，压实中止"
                            )
                    out.flush()
                    os.fsync(out.fileno())
                if written != expected_events:
                    raise CompactionError(
                        f"合并行数 {written} 与索引记录 {expected_events} 不一致，压实中止"
                    )
                # 第二遍独立校验：回读临时文件，核对行数与 sha256
                reread_hash = hashlib.sha256()
                reread_lines = 0
                with open(tmp, "rb") as inp:
                    for line in inp:
                        reread_hash.update(line)
                        reread_lines += 1
                if reread_lines != expected_events or reread_hash.hexdigest() != hasher.hexdigest():
                    raise CompactionError("合并文件回读校验失败，压实中止")
                self._fsync_dir(self.tmp_dir)

                final_path = self._event_path(compacted_name)
                os.replace(tmp, final_path)
                self._fsync_dir(self.events_dir)
            except CompactionError:
                self._safe_unlink(tmp)
                raise
            except Exception as exc:
                self._safe_unlink(tmp)
                raise CompactionError(f"压实失败: {exc}") from exc

            digest = hasher.hexdigest()
            size_bytes = final_path.stat().st_size
            source_ids = [item[0] for item in sources]
            source_names = [item[1] for item in sources]
            try:
                self._commit_compaction_swap(
                    partition_day, compacted_name, digest, expected_events,
                    size_bytes, source_ids,
                )
            except CompactionError:
                self._quarantine(final_path, f"compaction index swap failed: {partition_day}")
                raise
            except Exception as exc:
                # 切换失败：新文件隔离，旧文件全部保持 published 可读
                self._quarantine(final_path, f"compaction index swap failed: {partition_day}")
                raise CompactionError(f"压实索引切换失败: {exc}") from exc

            # 切换成功后才回收旧文件（等待在途查询结束）
            self._reap_superseded(source_names)
            return {
                "partition_day": partition_day,
                "compacted": True,
                "files_before": len(source_names),
                "files_after": self._published_count(partition_day),
                "events": expected_events,
                "filename": compacted_name,
                "sha256": digest,
            }

    def _published_count(self, partition_day: str) -> int:
        with self._read_lock:
            row = self._reader.execute(
                "SELECT COUNT(*) AS n FROM files "
                "WHERE partition_day = ? AND state = 'published'",
                (partition_day,),
            ).fetchone()
        return int(row["n"])

    # ------------------------------------------------------------------ 统计
    def stats(self, partition_day: str) -> dict:
        validate_day(partition_day)
        with self._read_lock:
            rows = self._reader.execute(
                "SELECT filename, event_count, size_bytes, state FROM files "
                "WHERE partition_day = ? ORDER BY state, id",
                (partition_day,),
            ).fetchall()
        published = [r for r in rows if r["state"] == "published"]
        return {
            "partition_day": partition_day,
            "published_files": len(published),
            "superseded_files": len(rows) - len(published),
            "total_events": sum(int(r["event_count"]) for r in published),
            "total_bytes": sum(int(r["size_bytes"]) for r in published),
            "files": [
                {
                    "filename": r["filename"],
                    "state": r["state"],
                    "event_count": r["event_count"],
                    "size_bytes": r["size_bytes"],
                }
                for r in rows
            ],
        }

    def close(self) -> None:
        with self._index_lock:
            self._writer.close()
        self._reader.close()


class ReadSession:
    """一个分区快照上的只读流式会话。"""

    def __init__(self, store: PartitionStore, partition_day: str, files: Sequence[str]) -> None:
        self._store = store
        self.partition_day = partition_day
        self.files = files

    def iter_lines(self) -> Iterator[bytes]:
        """按文件索引顺序逐行产出原始 NDJSON 字节。"""
        for filename in self.files:
            path = self._store._event_path(filename)
            with open(path, "rb") as inp:
                while True:
                    chunk = inp.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk

    @property
    def file_count(self) -> int:
        return len(self.files)
