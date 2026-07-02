"""统一日志入口，委托 tools.common_logger.logger_common。"""
from __future__ import annotations

import sys
from pathlib import Path


def _find_repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "tools" / "common_logger" / "logger_common.py").is_file():
            return parent
    raise RuntimeError("cannot find repo root containing tools/common_logger")


_REPO_ROOT = _find_repo_root()
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tools.common_logger.logger_common import (
    LoggerConfig,
    get_default_logger,
    get_logger,
    get_task_logger,
)

__all__ = [
    "LoggerConfig",
    "get_default_logger",
    "get_logger",
    "get_task_logger",
]
