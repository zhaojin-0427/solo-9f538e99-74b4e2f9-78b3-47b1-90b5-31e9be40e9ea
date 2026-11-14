"""运行配置，全部可通过环境变量覆盖。"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"环境变量 {name} 必须是整数，实际为: {raw!r}") from exc
    if value <= 0:
        raise RuntimeError(f"环境变量 {name} 必须为正整数")
    return value


@dataclass(frozen=True)
class Settings:
    data_dir: str
    # 单次上传体积上限（字节），默认 100 MiB
    max_upload_bytes: int
    # 压实后等待在途查询结束再回收旧文件的最长秒数；超时则留到下次回收
    reap_wait_seconds: int


def load_settings() -> Settings:
    """每次调用都从当前环境读取，便于测试与运行时覆盖。"""
    return Settings(
        data_dir=os.environ.get("DATA_DIR", "/data"),
        max_upload_bytes=_env_int("MAX_UPLOAD_BYTES", 100 * 1024 * 1024),
        reap_wait_seconds=_env_int("REAP_WAIT_SECONDS", 30),
    )


# 模块级默认值，供不关心环境的代码直接引用
settings = load_settings()
