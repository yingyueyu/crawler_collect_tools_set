from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

import pymysql
from pymysql.cursors import DictCursor

from .config import MySQLConfig


class SyncMySQLClient:
    def __init__(self, config: MySQLConfig, use_dict_cursor: bool = True) -> None:
        self.config = config
        self.use_dict_cursor = use_dict_cursor
        self._conn: Optional[pymysql.connections.Connection] = None

    def connect(self) -> "SyncMySQLClient":
        cursor_cls = DictCursor if self.use_dict_cursor else pymysql.cursors.Cursor
        self._conn = pymysql.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database or None,
            charset=self.config.charset,
            connect_timeout=self.config.connect_timeout,
            read_timeout=self.config.read_timeout,
            write_timeout=self.config.write_timeout,
            autocommit=self.config.autocommit,
            cursorclass=cursor_cls,
        )
        return self

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "SyncMySQLClient":
        if self._conn is None:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    @property
    def conn(self) -> pymysql.connections.Connection:
        if self._conn is None:
            raise RuntimeError("MySQL connection is not initialized. Call connect() first.")
        return self._conn

    @contextmanager
    def transaction(self) -> Iterator[None]:
        try:
            yield
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        with self.conn.cursor() as cur:
            affected = cur.execute(sql, params)
        if self.config.autocommit:
            return affected
        self.conn.commit()
        return affected

    def executemany(self, sql: str, params_list: Iterable[Sequence[Any]]) -> int:
        with self.conn.cursor() as cur:
            affected = cur.executemany(sql, list(params_list))
        if self.config.autocommit:
            return affected
        self.conn.commit()
        return affected

    def query_one(self, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        return row

    def query_all(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return list(rows)
