"""加载 MySQL 流式任务 profile JSON。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_PROFILES_DIR = Path(__file__).resolve().parent / "profiles"
_REPO_ROOT = Path(__file__).resolve().parents[2]

_REQUIRED_FIELDS = (
    "platform_key",
    "database_name",
    "table_name",
    "columns",
)

_OPTIONAL_DEFAULTS: dict[str, Any] = {
    "cursor_field": None,
    "conditions": None,
    "use_redis": True,
    "redis": {},
    "upload_format": {},
    "writeback": {},
    "batch_size": 1000,
    "monitor_interval": 2,
    "threshold_ratio": 0.2,
    "success_queue_stable_seconds": 30,
}


def _build_builtin_profiles() -> dict[str, Path]:
    profiles: dict[str, Path] = {}
    if _PROFILES_DIR.is_dir():
        for path in sorted(_PROFILES_DIR.glob("*.json")):
            profiles.setdefault(path.stem.lower(), path)
    return profiles


_BUILTIN_PROFILES = _build_builtin_profiles()


def get_repo_root() -> Path:
    return _REPO_ROOT


def list_builtin_platforms() -> list[str]:
    """返回 profiles/ 下已注册的内置平台 key（sorted）。"""
    return sorted(_BUILTIN_PROFILES.keys())


def resolve_profile_path(platform_or_path: str) -> Path:
    key = platform_or_path.strip().lower()
    if key in _BUILTIN_PROFILES:
        return _BUILTIN_PROFILES[key]
    profile_path = Path(platform_or_path)
    if not profile_path.is_absolute():
        profile_path = (_REPO_ROOT / profile_path).resolve()
    return profile_path


def normalize_profile_data(data: dict) -> dict:
    """校验必填字段并填充可选默认值。"""
    missing = [k for k in _REQUIRED_FIELDS if k not in data]
    if missing:
        raise ValueError(f"profile 缺少字段: {missing}")

    normalized = dict(data)
    for key, default in _OPTIONAL_DEFAULTS.items():
        normalized.setdefault(key, default)
    return normalized


def load_profile_data(platform_or_path: str) -> dict:
    profile_path = resolve_profile_path(platform_or_path)
    if not profile_path.is_file():
        builtins = ", ".join(list_builtin_platforms())
        raise FileNotFoundError(
            f"profile 不存在: {profile_path}；"
            f"可用内置平台: {builtins}；"
            f"或传相对路径如 tools/common_mysql/profiles/maven.json"
        )

    with profile_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    return normalize_profile_data(data)
