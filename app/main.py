"""FastAPI 接口层。

路由
====
POST /partitions/{day}/logs     上传指定日期的日志（NDJSON 原始字节流），
                                混入其他日期整份拒收
GET  /partitions/{day}/events   按日流式读取完整事件流水（NDJSON）
POST /partitions/{day}/compact  压实该分区
GET  /partitions/{day}/stats    查看分区索引与计数
GET  /healthz                   健康检查
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from contextlib import asynccontextmanager

from anyio import to_thread
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .config import load_settings
from .storage import (
    CompactionError,
    PartitionStore,
    RejectedUpload,
    UploadTooLarge,
    validate_day,
)

# 请求体先假脱机：小于 1 MiB 留内存，超出自动落临时文件，避免大文件占用进程内存
SPOOL_THRESHOLD = 1024 * 1024


@asynccontextmanager
async def lifespan(app: FastAPI) -> Iterator[None]:
    app.state.store = PartitionStore(config=load_settings())
    try:
        yield
    finally:
        app.state.store.close()


app = FastAPI(
    title="物流分拣扫码日志分区服务",
    version="1.0.0",
    description="按 UTC 日期分区的扫码日志上传、读取与压实服务（SQLite 索引 + NDJSON）",
    lifespan=lifespan,
)


def get_store(request: Request) -> PartitionStore:
    return request.app.state.store


def _check_day(day: str) -> str:
    try:
        return validate_day(day)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/healthz")
def healthz(request: Request) -> dict:
    store = get_store(request)
    return {"status": "ok", "data_dir": str(store.root)}


@app.post("/partitions/{day}/logs", status_code=201)
async def upload_log(day: str, request: Request) -> JSONResponse:
    """接收 application/x-ndjson 原始字节流，逐行流式校验并落盘。"""
    partition_day = _check_day(day)
    store = get_store(request)

    with tempfile.SpooledTemporaryFile(max_size=SPOOL_THRESHOLD) as spool:
        total = 0
        limit = store._settings.max_upload_bytes
        async for chunk in request.stream():
            if not chunk:
                continue
            total += len(chunk)
            if total > limit:
                raise HTTPException(status_code=413, detail=f"上传文件超过 {limit} 字节上限")
            spool.write(chunk)
        spool.flush()
        spool.seek(0)

        def line_iter() -> Iterator[bytes]:
            while True:
                line = spool.readline()
                if not line:
                    return
                yield line

        try:
            # 同步存储逻辑放到线程池，避免阻塞事件循环，保证多请求真正并行
            result = await to_thread.run_sync(store.upload, partition_day, line_iter())
        except UploadTooLarge as exc:
            raise HTTPException(status_code=413, detail=str(exc)) from exc
        except RejectedUpload as exc:
            detail = str(exc)
            if exc.line_no is not None:
                detail = f"第 {exc.line_no} 行: {detail}"
            raise HTTPException(status_code=422, detail=detail) from exc
    return JSONResponse(status_code=201, content=result)


@app.get("/partitions/{day}/events")
def read_events(day: str, request: Request) -> StreamingResponse:
    """流式返回该分区当前快照中的全部事件，输出仍为 NDJSON。"""
    partition_day = _check_day(day)
    store = get_store(request)
    session_cm = store.read_session(partition_day)
    session = session_cm.__enter__()

    def generator() -> Iterator[bytes]:
        try:
            yield from session.iter_lines()
        finally:
            session_cm.__exit__(None, None, None)

    return StreamingResponse(
        generator(),
        media_type="application/x-ndjson",
        headers={
            "X-Partition-Day": partition_day,
            "X-Partition-Files": str(session.file_count),
        },
    )


@app.post("/partitions/{day}/compact")
def compact_partition(day: str, request: Request) -> dict:
    # 同步 def：FastAPI 自动放到线程池执行，压实期间不阻塞事件循环
    partition_day = _check_day(day)
    store = get_store(request)
    try:
        return store.compact(partition_day)
    except CompactionError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/partitions/{day}/stats")
def partition_stats(day: str, request: Request) -> dict:
    partition_day = _check_day(day)
    return get_store(request).stats(partition_day)
