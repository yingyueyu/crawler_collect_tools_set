from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.key_token_config import MYSQL_TOOLS_DEFAULT

_DEFAULT = MYSQL_TOOLS_DEFAULT


@dataclass(frozen=True)
class MySQLConfig:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = ""
    charset: str = "utf8mb4"
    connect_timeout: int = 10
    # 大批量 JSON 写库时建议 300+，避免远端或 Windows 套接字过早判定超时
    read_timeout: int = 300
    write_timeout: int = 300
    autocommit: bool = False
    minsize: int = 1
    maxsize: int = 10
    # 连接在池中闲置超过该秒数后重建，减轻 wait_timeout 导致的「僵尸连接」
    pool_recycle: int = 1800

    @classmethod
    def from_env(cls, prefix: str = "MYSQL_") -> "MySQLConfig":
        def _get_str(name: str, default: str) -> str:
            return os.getenv(prefix + name, default)

        def _get_int(name: str, default: int) -> int:
            raw = os.getenv(prefix + name)
            if raw is None or raw == "":
                return default
            return int(raw)

        def _get_bool(name: str, default: bool) -> bool:
            raw: Optional[str] = os.getenv(prefix + name)
            if raw is None:
                return default
            return raw.strip().lower() in {"1", "true", "yes", "on"}

        return cls(
            host=_get_str("HOST", _DEFAULT["host"]),
            port=_get_int("PORT", _DEFAULT["port"]),
            user=_get_str("USER", _DEFAULT["user"]),
            password=_get_str("PASSWORD", _DEFAULT["password"]),
            database=_get_str("DATABASE", ""),
            charset=_get_str("CHARSET", "utf8mb4"),
            connect_timeout=_get_int("CONNECT_TIMEOUT", 10),
            read_timeout=_get_int("READ_TIMEOUT", 300),
            write_timeout=_get_int("WRITE_TIMEOUT", 300),
            autocommit=_get_bool("AUTOCOMMIT", False),
            minsize=_get_int("MINSIZE", 1),
            maxsize=_get_int("MAXSIZE", 10),
            pool_recycle=_get_int("POOL_RECYCLE", 1800),
        )
