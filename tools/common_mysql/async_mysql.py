from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import aiomysql
import pymysql.err

from .config import MySQLConfig


class AsyncMySQLClient:
    def __init__(self, config: MySQLConfig, use_dict_cursor: bool = True) -> None:
        self.config = config
        self.use_dict_cursor = use_dict_cursor
        self._pool: Optional[aiomysql.Pool] = None

    async def connect(self) -> "AsyncMySQLClient":
        cursor_cls = aiomysql.DictCursor if self.use_dict_cursor else aiomysql.Cursor
        # 注意：部分 aiomysql 版本底层 connect() 不支持 read_timeout/write_timeout，
        # 传入会触发 TypeError；超时控制依赖 executemany_chunked、pool_recycle 与升级 aiomysql/pymysql。
        self._pool = await aiomysql.create_pool(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            db=self.config.database or None,
            charset=self.config.charset,
            connect_timeout=self.config.connect_timeout,
            autocommit=self.config.autocommit,
            minsize=self.config.minsize,
            maxsize=self.config.maxsize,
            pool_recycle=self.config.pool_recycle,
            cursorclass=cursor_cls,
        )
        return self

    async def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def __aenter__(self) -> "AsyncMySQLClient":
        if self._pool is None:
            await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    @property
    def pool(self) -> aiomysql.Pool:
        if self._pool is None:
            raise RuntimeError("MySQL pool is not initialized. Call connect() first.")
        return self._pool

    @asynccontextmanager
    async def transaction(self):
        async with self.pool.acquire() as conn:
            await conn.begin()
            try:
                yield conn
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

    async def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                affected = await cur.execute(sql, params)
            if not self.config.autocommit:
                await conn.commit()
            return affected

    async def executemany(self, sql: str, params_list: Iterable[Sequence[Any]]) -> int:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                affected = await cur.executemany(sql, list(params_list))
            if not self.config.autocommit:
                await conn.commit()
            return affected

    async def executemany_chunked(
        self,
        sql: str,
        params_list: Iterable[Sequence[Any]],
        *,
        chunk_size: int = 40,
        reconnect_on_lost: bool = True,
    ) -> int:
        """
        分块 executemany，减轻单次包过大或连接闲置过久导致的 Lost connection (2013) / WinError 121。
        """
        items = list(params_list)
        if not items:
            return 0
        chunk_size = max(1, int(chunk_size))
        total_affected = 0
        for i in range(0, len(items), chunk_size):
            chunk = items[i : i + chunk_size]
            try:
                total_affected += await self.executemany(sql, chunk)
            except pymysql.err.OperationalError as exc:
                if (
                    reconnect_on_lost
                    and exc.args
                    and exc.args[0] == 2013
                ):
                    await self.close()
                    await self.connect()
                    total_affected += await self.executemany(sql, chunk)
                else:
                    raise
        return total_affected

    async def executemany_chunked_parallel(
        self,
        sql: str,
        params_list: Iterable[Sequence[Any]],
        *,
        chunk_size: int = 50,
        max_concurrency: int = 8,
        lock_retries: int = 5,
        lock_retry_base_sec: float = 0.2,
    ) -> int:
        """
        将参数切成多块，在连接池上并发 executemany，提高写吞吐（每块仍单独 commit）。

        并发过高或单块过大时 InnoDB 易出现 1205（Lock wait timeout）/ 1213（死锁），
        已对这两种错误做有限次指数退避重试；仍失败时请降低 max_concurrency、减小 chunk_size，
        或对本客户端启用 autocommit=True（每条语句尽快释放锁）。
        """
        items = list(params_list)
        if not items:
            return 0
        chunk_size = max(1, int(chunk_size))
        max_concurrency = max(1, int(max_concurrency))
        lock_retries = max(1, int(lock_retries))
        chunks: List[List[Sequence[Any]]] = [
            items[i : i + chunk_size] for i in range(0, len(items), chunk_size)
        ]
        sem = asyncio.Semaphore(max_concurrency)

        async def _run_chunk(chunk: List[Sequence[Any]]) -> int:
            async with sem:
                delay = float(lock_retry_base_sec)
                for attempt in range(lock_retries):
                    try:
                        return await self.executemany(sql, chunk)
                    except pymysql.err.OperationalError as exc:
                        errno = exc.args[0] if exc.args else None
                        if errno in (1205, 1213) and attempt + 1 < lock_retries:
                            await asyncio.sleep(delay)
                            delay *= 2
                            continue
                        raise

        results = await asyncio.gather(*[_run_chunk(c) for c in chunks])
        return int(sum(results))

    async def bulk_update_case_by_pk(
        self,
        table_ref: str,
        pk_column: str,
        value_column: str,
        pairs: List[Tuple[str, str]],
        *,
        only_when_value_null: bool = True,
    ) -> int:
        """
        单条 UPDATE，用 CASE WHEN pk THEN value 批量改多行，显著减少网络 RTT（相对逐行 executemany）。

        :param pairs: (主键值, 新字段值)，主键列由 pk_column 指定
        """
        if not pairs:
            return 0
        case_parts: List[str] = []
        params: List[Any] = []
        for pk_val, new_val in pairs:
            case_parts.append("WHEN %s THEN %s")
            params.extend([pk_val, new_val])
        case_sql = " ".join(case_parts)
        in_ph = ",".join(["%s"] * len(pairs))
        for pk_val, _ in pairs:
            params.append(pk_val)
        null_guard = f" AND `{value_column}` IS NULL" if only_when_value_null else ""
        sql = (
            f"UPDATE {table_ref} SET `{value_column}` = CASE `{pk_column}` {case_sql} END "
            f"WHERE `{pk_column}` IN ({in_ph}){null_guard}"
        )
        return await self.execute(sql, tuple(params))

    async def bulk_update_case_by_pk_chunked_parallel(
        self,
        table_ref: str,
        pk_column: str,
        value_column: str,
        pairs: List[Tuple[str, str]],
        *,
        rows_per_statement: int = 100,
        max_concurrency: int = 4,
        only_when_value_null: bool = True,
        lock_retries: int = 5,
        lock_retry_base_sec: float = 0.25,
        progress_cb: Optional[Callable[[int, int, int, int], None]] = None,
        progress_every_chunks: int = 1,
    ) -> int:
        """
        将 pairs 切成多段，每段一条 CASE UPDATE；多段之间并发执行。
        rows_per_statement 过大可能触发 max_allowed_packet，可先降到 40～60。

        :param progress_cb: 每完成若干 SQL 块后回调
            ``(completed_rows, total_rows, completed_chunks, total_chunks)``，在协程内线程安全调用。
        :param progress_every_chunks: 每完成多少个 SQL 块触发一次 progress_cb（最后一块总会触发）。
        """
        items = list(pairs)
        if not items:
            return 0
        rows_per_statement = max(1, int(rows_per_statement))
        max_concurrency = max(1, int(max_concurrency))
        lock_retries = max(1, int(lock_retries))
        progress_every_chunks = max(1, int(progress_every_chunks))
        chunks: List[List[Tuple[str, str]]] = [
            items[i : i + rows_per_statement] for i in range(0, len(items), rows_per_statement)
        ]
        total_row_count = len(items)
        total_chunk_count = len(chunks)
        sem = asyncio.Semaphore(max_concurrency)
        prog_lock = asyncio.Lock()
        completed_rows = 0
        completed_chunks = 0

        async def _run(ch: List[Tuple[str, str]]) -> int:
            async with sem:
                delay = float(lock_retry_base_sec)
                for attempt in range(lock_retries):
                    try:
                        return await self.bulk_update_case_by_pk(
                            table_ref,
                            pk_column,
                            value_column,
                            ch,
                            only_when_value_null=only_when_value_null,
                        )
                    except pymysql.err.OperationalError as exc:
                        errno = exc.args[0] if exc.args else None
                        if errno in (1205, 1213, 2013) and attempt + 1 < lock_retries:
                            await asyncio.sleep(delay)
                            delay *= 2
                            continue
                        raise

        async def _run_and_track(ch: List[Tuple[str, str]]) -> int:
            nonlocal completed_rows, completed_chunks
            res = await _run(ch)
            async with prog_lock:
                completed_rows += len(ch)
                completed_chunks += 1
                if progress_cb is not None:
                    last = completed_chunks == total_chunk_count
                    if (
                        completed_chunks % progress_every_chunks == 0
                        or last
                    ):
                        progress_cb(
                            completed_rows,
                            total_row_count,
                            completed_chunks,
                            total_chunk_count,
                        )
            return res

        results = await asyncio.gather(*[_run_and_track(c) for c in chunks])
        return int(sum(results))

    async def query_one(self, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                row = await cur.fetchone()
        return row

    async def query_all(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                rows = await cur.fetchall()
        return list(rows)
