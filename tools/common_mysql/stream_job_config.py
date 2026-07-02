"""MySQL/Redis 流式任务共享配置与工具函数。"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import aiomysql

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tools.common_mysql.config import MySQLConfig

MYSQL_CFG = MySQLConfig.from_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


@dataclass
class RedisSettings:
    host: str = "127.0.0.1"
    port: int = 6379
    db: int = 0
    password: str | None = None
    queue_key: str = ""
    success_queue_key: str = ""


@dataclass
class UploadFormatConfig:
    """Redis 队列单条 payload 格式。"""

    type: str = "text"
    field: str | list[str] = ""

    @classmethod
    def from_dict(cls, data: dict | None) -> UploadFormatConfig:
        if not data:
            return cls()
        typ = str(data.get("type", "text")).strip().lower()
        raw_field = data.get("field")
        if raw_field is None:
            raw_field = data.get("fields") or data.get("filed") or ""
        return cls(type=typ, field=raw_field)


@dataclass
class WritebackConfig:
    """写回 MySQL 时的主键与 UPDATE 字段。"""

    id_field: str = "purl"
    pending_condition: str = "is_finish = 0"
    update_fields: dict[str, Any] = field(
        default_factory=lambda: {"is_finish": 1, "updated_time": "NOW()"}
    )


@dataclass
class StreamJobConfig:
    database: str
    table: str
    columns: str
    conditions: str | dict | list | None = None
    cursor_field: str | None = None
    use_redis: bool = True
    redis: RedisSettings = field(default_factory=RedisSettings)
    upload_format: UploadFormatConfig = field(default_factory=UploadFormatConfig)
    writeback: WritebackConfig = field(default_factory=WritebackConfig)
    batch_size: int = 1000
    monitor_interval: int = 2
    threshold_ratio: float = 0.2
    success_queue_stable_seconds: int = 30


def mysql_config_to_dict(cfg: MySQLConfig, database: str | None = None) -> dict:
    return {
        "host": cfg.host,
        "port": cfg.port,
        "user": cfg.user,
        "password": cfg.password,
        "database": database or cfg.database,
    }


async def create_mysql_pool(mysql_config: dict, cfg: MySQLConfig | None = None):
    pool_cfg = cfg or MYSQL_CFG
    return await aiomysql.create_pool(
        host=mysql_config["host"],
        port=mysql_config["port"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        db=mysql_config["database"],
        charset=pool_cfg.charset,
        autocommit=True,
        minsize=pool_cfg.minsize,
        maxsize=pool_cfg.maxsize,
        connect_timeout=pool_cfg.connect_timeout,
        pool_recycle=pool_cfg.pool_recycle,
    )


async def redis_call(result):
    if asyncio.iscoroutine(result):
        return await result
    return result


async def close_redis_client(client):
    if client is None:
        return
    close_fn = getattr(client, "aclose", None) or client.close
    result = close_fn()
    if asyncio.iscoroutine(result):
        await result


def resolve_id_field(columns: str, id_field: str | None = None) -> str:
    if id_field:
        return id_field
    if columns and columns.strip() != "*":
        parts = [p.strip() for p in columns.split(",") if p.strip()]
        if len(parts) == 1:
            return parts[0]
    return "lower_purl"


def build_where_clause(conditions=None) -> tuple[str, list]:
    if conditions is None:
        return "", []

    if isinstance(conditions, str):
        return conditions, []

    if isinstance(conditions, dict):
        clauses = []
        params = []
        for key, value in conditions.items():
            clauses.append(f"{key} = %s")
            params.append(value)
        if clauses:
            return " AND ".join(clauses), params
        return "", []

    if isinstance(conditions, list):
        clauses = []
        params = []
        for field_name, operator, value in conditions:
            clauses.append(f"{field_name} {operator} %s")
            params.append(value)
        if clauses:
            return " AND ".join(clauses), params
        return "", []

    logger.warning("不支持的条件格式: %s", type(conditions))
    return "", []


def format_redis_payload(row: dict, fmt: UploadFormatConfig) -> str:
    if fmt.type == "dict":
        field_list = fmt.field if isinstance(fmt.field, list) else [fmt.field]
        field_list = [f for f in field_list if f]
        payload = {name: row.get(name) for name in field_list}
        return json.dumps(payload, ensure_ascii=False)
    field_name = fmt.field if isinstance(fmt.field, str) else (fmt.field[0] if fmt.field else "")
    return str(row.get(field_name, ""))


def build_update_set_clause(update_fields: dict[str, Any]) -> tuple[str, list]:
    set_parts: list[str] = []
    params: list[Any] = []
    for key, value in update_fields.items():
        if isinstance(value, str) and value.upper() == "NOW()":
            set_parts.append(f"{key} = NOW()")
        else:
            set_parts.append(f"{key} = %s")
            params.append(value)
    return ", ".join(set_parts), params


def normalize_queue_id(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return value
    if value.startswith("pkg:"):
        return value

    maven_match = re.match(
        r"https?://(?:www\.)?mvnrepository\.com/artifact/([^/?#]+)/([^/?#]+)",
        value,
        re.I,
    )
    if maven_match:
        group_id = unquote(maven_match.group(1))
        artifact_id = unquote(maven_match.group(2))
        return f"pkg:maven/{group_id}/{artifact_id}"

    npm_match = re.match(r"https?://www\.npmjs\.com/package/(.+)", value, re.I)
    if npm_match:
        return f"pkg:npm/{unquote(npm_match.group(1))}"

    pypi_match = re.match(r"https?://pypi\.org/project/([^/?#]+)", value, re.I)
    if pypi_match:
        return f"pkg:pypi/{unquote(pypi_match.group(1))}"

    return value


def extract_id_from_queue_item(
    item: str,
    id_field: str,
    upload_format: UploadFormatConfig,
) -> str:
    if upload_format.type == "dict":
        try:
            data = json.loads(item)
            if isinstance(data, dict) and id_field in data:
                return str(data[id_field])
        except json.JSONDecodeError:
            pass
    return normalize_queue_id(item)


def _build_redis_settings(data: dict | None) -> RedisSettings:
    raw = data or {}
    return RedisSettings(
        host=str(raw.get("host", "127.0.0.1")),
        port=int(raw.get("port", 6379)),
        db=int(raw.get("db", 0)),
        password=raw.get("password"),
        queue_key=str(raw.get("queue_key", "")),
        success_queue_key=str(raw.get("success_queue_key", "")),
    )


def _build_writeback_config(data: dict | None) -> WritebackConfig:
    raw = data or {}
    update_fields = raw.get("update_fields")
    if update_fields is None:
        update_fields = {"is_finish": 1, "updated_time": "NOW()"}
    return WritebackConfig(
        id_field=str(raw.get("id_field", "purl")),
        pending_condition=str(raw.get("pending_condition", "is_finish = 0")),
        update_fields=update_fields,
    )


def build_stream_job_config(profile_data: dict) -> StreamJobConfig:
    """将 profile 字典转为 StreamJobConfig。"""
    return StreamJobConfig(
        database=str(profile_data["database_name"]),
        table=str(profile_data["table_name"]),
        columns=str(profile_data["columns"]),
        conditions=profile_data.get("conditions"),
        cursor_field=profile_data.get("cursor_field"),
        use_redis=bool(profile_data.get("use_redis", True)),
        redis=_build_redis_settings(profile_data.get("redis")),
        upload_format=UploadFormatConfig.from_dict(profile_data.get("upload_format")),
        writeback=_build_writeback_config(profile_data.get("writeback")),
        batch_size=int(profile_data.get("batch_size", 1000)),
        monitor_interval=int(profile_data.get("monitor_interval", 2)),
        threshold_ratio=float(profile_data.get("threshold_ratio", 0.2)),
        success_queue_stable_seconds=int(
            profile_data.get("success_queue_stable_seconds", 30)
        ),
    )


def resolve_platform_key(args: argparse.Namespace) -> str:
    if args.profile:
        return args.profile
    if args.platform:
        return args.platform
    raise SystemExit("请指定 --platform 或 --profile")
