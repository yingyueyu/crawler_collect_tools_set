"""
MySQL 表数据流式读取工具：可选上传 Redis 任务队列，或直接写回 MySQL。

适配 tools.common_mysql.MySQLConfig；任务参数通过 CLI + profiles/*.json 配置。

示例::

  # 使用内置 maven 平台配置
  python tools/common_mysql/write_data_from_db.py --platform maven

  # 使用自定义 profile JSON
  python tools/common_mysql/write_data_from_db.py --profile path/to/custom.json

  # 覆盖部分参数
  python tools/common_mysql/write_data_from_db.py --platform maven --batch-size 500

  # 非 Redis 模式（仅读取 + 外部处理写回，见 example_non_redis_process_batch）
  python tools/common_mysql/write_data_from_db.py --platform maven --no-redis

Redis 上传格式示例（查询字段 purl, repo_id）::
    {"type": "dict", "field": ["purl", "repo_id"]}
    {"type": "text", "field": "repo_id"}

写回模式::
    - use_redis=True   : MySQL -> Redis 任务队列；SuccessQueueMonitor 消费成功队列后 UPDATE
    - use_redis=False  : MysqlTableReader 只负责读取；外部流程处理完后调用
                         MysqlWritebackClient.writeback(...) 写回

仅消费成功队列（独立入口，不依赖本模块）::

    python tools/common_mysql/drain_success_queue_to_mysql.py --platform maven

非 Redis 典型用法::

    async def my_process_batch(rows, writeback, job):
        success_ids = []
        for row in rows:
            ok = await external_api_call(row)
            if ok:
                success_ids.append(row[job.writeback.id_field])
        if success_ids:
            await writeback.writeback(job.table, success_ids)

    await run_non_redis_pipeline(job, my_process_batch)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import aiomysql
import redis.asyncio as redis
from pymysql.err import OperationalError

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tools.common_mysql.config import MySQLConfig
from tools.common_mysql.stream_job_config import (
    MYSQL_CFG,
    RedisSettings,
    StreamJobConfig,
    UploadFormatConfig,
    WritebackConfig,
    build_stream_job_config,
    build_update_set_clause,
    build_where_clause,
    close_redis_client,
    create_mysql_pool,
    format_redis_payload,
    logger,
    mysql_config_to_dict,
    redis_call,
    resolve_id_field,
    resolve_platform_key,
)
from tools.common_mysql.success_queue import SuccessQueueMonitor


# ---------------------------------------------------------------------------
# MySQL -> Redis
# ---------------------------------------------------------------------------


class MySQLToRedisStreamer:
    def __init__(
        self,
        mysql_config: dict,
        redis_settings: RedisSettings,
        upload_format: UploadFormatConfig | None = None,
        batch_size: int = 500,
        monitor_interval: int = 5,
        threshold_ratio: float = 0.1,
        mysql_cfg: MySQLConfig | None = None,
    ):
        self.mysql_config = mysql_config
        self.redis_settings = redis_settings
        self.upload_format = upload_format or UploadFormatConfig()
        self.batch_size = batch_size
        self.monitor_interval = monitor_interval
        self.threshold = int(batch_size * threshold_ratio)
        self.mysql_cfg = mysql_cfg or MYSQL_CFG
        self.redis_client = None
        self.mysql_pool = None
        self.stop_event = asyncio.Event()
        self.last_cursor = ""
        self.cursor_field = "lower_purl"
        self.total_count = 0
        self.uploaded_count = 0
        self.where_clause = ""
        self.where_params: list = []
        self.start_time = 0

    async def init_connections(self):
        redis_kwargs = {
            "host": self.redis_settings.host,
            "port": self.redis_settings.port,
            "db": self.redis_settings.db,
            "decode_responses": True,
        }
        if self.redis_settings.password:
            redis_kwargs["password"] = self.redis_settings.password

        self.redis_client = redis.Redis(**redis_kwargs)
        await redis_call(self.redis_client.ping())
        logger.info(
            "成功连接 Redis: %s:%s db=%s",
            self.redis_settings.host,
            self.redis_settings.port,
            self.redis_settings.db,
        )

        self.mysql_pool = await create_mysql_pool(self.mysql_config, self.mysql_cfg)
        logger.info(
            "成功连接 MySQL: %s:%s db=%s",
            self.mysql_config["host"],
            self.mysql_config["port"],
            self.mysql_config["database"],
        )

    async def get_total_count(self, table_name: str, conn=None) -> int:
        async def _query(cur):
            if self.where_clause:
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {self.where_clause}"
                await cur.execute(query, tuple(self.where_params))
            else:
                await cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            result = await cur.fetchone()
            return result[0] if result else 0

        if conn is not None:
            async with conn.cursor() as cur:
                return await _query(cur)

        async with self.mysql_pool.acquire() as acquired:
            async with acquired.cursor() as cur:
                return await _query(cur)

    async def stream_query(self, table_name: str, columns: str = "*"):
        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if self.start_time == 0:
                    self.start_time = time.time()

                while True:
                    if self.where_clause:
                        query = (
                            f"SELECT {columns} FROM {table_name} "
                            f"WHERE ({self.where_clause}) AND {self.cursor_field} > %s "
                            f"ORDER BY {self.cursor_field} ASC LIMIT %s"
                        )
                        params = tuple(self.where_params) + (self.last_cursor, self.batch_size)
                    else:
                        query = (
                            f"SELECT {columns} FROM {table_name} "
                            f"WHERE {self.cursor_field} > %s "
                            f"ORDER BY {self.cursor_field} ASC LIMIT %s"
                        )
                        params = (self.last_cursor, self.batch_size)

                    await cur.execute(query, params)
                    rows = await cur.fetchmany(self.batch_size)
                    if not rows:
                        break

                    yield rows
                    self.last_cursor = rows[-1][self.cursor_field]
                    self.uploaded_count += len(rows)

                    elapsed = time.time() - self.start_time
                    speed = self.uploaded_count / elapsed if elapsed > 0 else 0
                    remaining_in_db = await self.get_total_count(table_name, conn=conn)
                    logger.info(
                        "已读取 %s 条 | DB 剩余约 %s | 游标 %s>%.60s | 速度 %.2f 条/秒",
                        self.uploaded_count,
                        remaining_in_db,
                        self.cursor_field,
                        self.last_cursor,
                        speed,
                    )

    async def upload_batch(self, queue_key: str, data_batch: list):
        if not data_batch:
            return

        payloads = [format_redis_payload(item, self.upload_format) for item in data_batch]
        payloads = [p for p in payloads if p]
        unique_payloads = list(dict.fromkeys(payloads))
        dup_count = len(payloads) - len(unique_payloads)

        if unique_payloads:
            push_pipe = self.redis_client.pipeline()
            for payload in unique_payloads:
                push_pipe.lpush(queue_key, payload)
            await redis_call(push_pipe.execute())

        msg = f"成功上传 {len(unique_payloads)} 条到队列 {queue_key} (format={self.upload_format.type})"
        if dup_count:
            msg += f"（本批去重 {dup_count} 条）"
        logger.info(msg)

    async def monitor_queue(self, queue_key: str) -> int:
        try:
            return await redis_call(self.redis_client.llen(queue_key))
        except Exception as exc:
            logger.error("获取队列长度失败: %s", exc)
            return -1

    async def close(self):
        await close_redis_client(self.redis_client)
        self.redis_client = None
        if self.mysql_pool:
            self.mysql_pool.close()
            await self.mysql_pool.wait_closed()
            self.mysql_pool = None

    async def run_upload_only(self, job: StreamJobConfig):
        self.cursor_field = resolve_id_field(job.columns, job.cursor_field)
        logger.info("游标字段: %s | Redis 上传格式: %s", self.cursor_field, job.upload_format)

        await self.init_connections()
        self.where_clause, self.where_params = build_where_clause(job.conditions)
        if self.where_clause:
            logger.info("查询条件: WHERE %s", self.where_clause)

        self.total_count = await self.get_total_count(job.table)
        logger.info("表 %s 符合条件的记录共有 %s 条", job.table, self.total_count)
        if self.total_count == 0:
            logger.warning("没有符合条件的数据，跳过 Redis 上传")
            return

        queue_key = job.redis.queue_key
        stream_generator = self.stream_query(job.table, job.columns)

        batch = None
        async for data_batch in stream_generator:
            batch = data_batch
            await self.upload_batch(queue_key, batch)
            break

        if not batch:
            return

        async for data_batch in stream_generator:
            while not self.stop_event.is_set():
                queue_length = await self.monitor_queue(queue_key)
                if queue_length <= self.threshold:
                    break
                await asyncio.sleep(job.monitor_interval)
            if self.stop_event.is_set():
                break
            await self.upload_batch(queue_key, data_batch)

        logger.info("Redis 上传完成，共 %s 条", self.uploaded_count)

    def stop(self):
        self.stop_event.set()



# ---------------------------------------------------------------------------
# 非 Redis：只读 + 独立写回（中间衔接外部处理流程）
# ---------------------------------------------------------------------------

BatchHandler = Callable[
    [list[dict], "MysqlWritebackClient", StreamJobConfig],
    Awaitable[None],
]


class MysqlTableReader:
    """非 Redis 模式：仅从 MySQL 游标分页读取，不负责写回。"""

    def __init__(
        self,
        mysql_config: dict,
        batch_size: int = 1000,
        mysql_cfg: MySQLConfig | None = None,
    ):
        self.mysql_config = mysql_config
        self.batch_size = batch_size
        self.mysql_cfg = mysql_cfg or MYSQL_CFG
        self.mysql_pool = None
        self.last_cursor = ""
        self.cursor_field = "purl"
        self.where_clause = ""
        self.where_params: list = []
        self.total_read = 0

    async def init_connections(self):
        self.mysql_pool = await create_mysql_pool(self.mysql_config, self.mysql_cfg)
        logger.info(
            "MysqlTableReader 已连接 MySQL %s:%s db=%s",
            self.mysql_config["host"],
            self.mysql_config["port"],
            self.mysql_config["database"],
        )

    async def close(self):
        if self.mysql_pool:
            self.mysql_pool.close()
            await self.mysql_pool.wait_closed()
            self.mysql_pool = None

    def _prepare_job(self, job: StreamJobConfig):
        self.cursor_field = resolve_id_field(
            job.columns, job.cursor_field or job.writeback.id_field
        )
        self.where_clause, self.where_params = build_where_clause(job.conditions)
        self.last_cursor = ""
        self.total_read = 0

    async def iter_batches(self, job: StreamJobConfig):
        """按游标分页 yield 批次行数据，供外部流程消费。"""
        if self.mysql_pool is None:
            raise RuntimeError("请先调用 init_connections()")

        self._prepare_job(job)
        logger.info(
            "MysqlTableReader 开始读取 | 表=%s | 字段=%s | 游标=%s",
            job.table,
            job.columns,
            self.cursor_field,
        )

        while True:
            async with self.mysql_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if self.where_clause:
                        query = (
                            f"SELECT {job.columns} FROM {job.table} "
                            f"WHERE ({self.where_clause}) AND {self.cursor_field} > %s "
                            f"ORDER BY {self.cursor_field} ASC LIMIT %s"
                        )
                        params = tuple(self.where_params) + (
                            self.last_cursor,
                            job.batch_size,
                        )
                    else:
                        query = (
                            f"SELECT {job.columns} FROM {job.table} "
                            f"WHERE {self.cursor_field} > %s "
                            f"ORDER BY {self.cursor_field} ASC LIMIT %s"
                        )
                        params = (self.last_cursor, job.batch_size)

                    await cur.execute(query, params)
                    rows = await cur.fetchmany(job.batch_size)

            if not rows:
                break

            self.last_cursor = rows[-1][self.cursor_field]
            self.total_read += len(rows)
            logger.info("MysqlTableReader 本批读取 %s 条，累计 %s", len(rows), self.total_read)
            yield rows


class MysqlWritebackClient:
    """
    独立 MySQL 写回客户端。

    在外部处理流程完成后，传入主键 id 列表或行字典列表，按 WritebackConfig 执行 UPDATE。
    """

    def __init__(
        self,
        mysql_config: dict,
        writeback: WritebackConfig,
        mysql_cfg: MySQLConfig | None = None,
        update_chunk_size: int = 500,
        update_max_retries: int = 3,
    ):
        self.mysql_config = mysql_config
        self.writeback = writeback
        self.mysql_cfg = mysql_cfg or MYSQL_CFG
        self.update_chunk_size = update_chunk_size
        self.update_max_retries = update_max_retries
        self.mysql_pool = None
        self.total_written = 0
        self.total_skipped = 0

    async def init_connections(self):
        self.mysql_pool = await create_mysql_pool(self.mysql_config, self.mysql_cfg)
        logger.info(
            "MysqlWritebackClient 已连接 MySQL %s:%s db=%s",
            self.mysql_config["host"],
            self.mysql_config["port"],
            self.mysql_config["database"],
        )

    async def close(self):
        if self.mysql_pool:
            self.mysql_pool.close()
            await self.mysql_pool.wait_closed()
            self.mysql_pool = None

    def _normalize_input(self, data: str | dict) -> str | None:
        if isinstance(data, str):
            return data.strip() or None
        if isinstance(data, dict):
            value = data.get(self.writeback.id_field)
            return str(value).strip() if value is not None else None
        return None

    def _dedupe_ids(self, data: list[str | dict]) -> list[str]:
        ids: list[str] = []
        seen: set[str] = set()
        for item in data:
            normalized = self._normalize_input(item)
            if normalized and normalized not in seen:
                seen.add(normalized)
                ids.append(normalized)
        return ids

    async def _filter_pending_ids(self, table_name: str, ids: list[str]) -> list[str]:
        if not ids:
            return []

        pending: list[str] = []
        chunks = [
            ids[i : i + self.update_chunk_size]
            for i in range(0, len(ids), self.update_chunk_size)
        ]
        for chunk in chunks:
            placeholders = ",".join(["%s"] * len(chunk))
            query = (
                f"SELECT {self.writeback.id_field} FROM {table_name} "
                f"WHERE {self.writeback.id_field} IN ({placeholders}) "
                f"AND ({self.writeback.pending_condition})"
            )
            async with self.mysql_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, tuple(chunk))
                    rows = await cur.fetchall()
                    pending.extend(str(row[0]) for row in rows)
        return pending

    async def _execute_update_chunk(self, table_name: str, ids: list[str]) -> int:
        placeholders = ",".join(["%s"] * len(ids))
        set_clause, set_params = build_update_set_clause(self.writeback.update_fields)
        sql = f"""
            UPDATE {table_name}
            SET {set_clause}
            WHERE {self.writeback.id_field} IN ({placeholders})
              AND ({self.writeback.pending_condition})
        """
        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, tuple(set_params) + tuple(ids))
                return cur.rowcount

    async def writeback(
        self,
        table_name: str,
        data: list[str | dict],
        *,
        skip_pending_filter: bool = False,
    ) -> int:
        """
        将外部处理完成的数据写回 MySQL。

        :param data: 主键字符串列表，或包含 id_field 的字典列表
        :param skip_pending_filter: True 时跳过 pending_condition 预过滤
        :return: 实际 UPDATE 影响行数
        """
        if self.mysql_pool is None:
            raise RuntimeError("请先调用 init_connections()")
        if not data:
            return 0

        unique_ids = self._dedupe_ids(data)
        if not unique_ids:
            return 0

        if skip_pending_filter:
            pending_ids = unique_ids
            skipped = 0
        else:
            pending_ids = await self._filter_pending_ids(table_name, unique_ids)
            skipped = len(unique_ids) - len(pending_ids)
            if skipped:
                self.total_skipped += skipped

        if not pending_ids:
            logger.info("写回跳过: 无待更新记录 (输入 %s 条)", len(unique_ids))
            return 0

        total_affected = 0
        chunks = [
            pending_ids[i : i + self.update_chunk_size]
            for i in range(0, len(pending_ids), self.update_chunk_size)
        ]
        for chunk_index, chunk in enumerate(chunks, start=1):
            for attempt in range(1, self.update_max_retries + 1):
                try:
                    affected = await self._execute_update_chunk(table_name, chunk)
                    total_affected += affected
                    logger.info(
                        "MysqlWritebackClient 写回 %s 条 (分片 %s/%s, 输入 %s)",
                        affected,
                        chunk_index,
                        len(chunks),
                        len(chunk),
                    )
                    break
                except OperationalError as exc:
                    if attempt >= self.update_max_retries:
                        raise
                    await asyncio.sleep(attempt * 2)
                    logger.warning(
                        "写回重试 %s/%s: %s",
                        attempt,
                        self.update_max_retries,
                        exc,
                    )

        self.total_written += total_affected
        return total_affected

    async def writeback_records(
        self,
        table_name: str,
        records: list[dict],
        *,
        update_columns: list[str] | None = None,
        always_set: dict[str, Any] | None = None,
        skip_pending_filter: bool = False,
    ) -> int:
        """
        按行写回不同字段值（CASE WHEN 批量 UPDATE）。

        :param records: 每条记录需包含 id_field 及待更新列
        :param update_columns: 要更新的列名；默认取 records 中除 id_field 外的键
        :param always_set: 每条记录统一追加的列，如 {"updated_time": "NOW()"}
        """
        if self.mysql_pool is None:
            raise RuntimeError("请先调用 init_connections()")
        if not records:
            return 0

        id_field = self.writeback.id_field
        normalized: dict[str, dict] = {}
        for row in records:
            pk = row.get(id_field)
            if pk is None:
                continue
            pk_str = str(pk).strip()
            if pk_str:
                normalized[pk_str] = row

        if not normalized:
            return 0

        record_list = list(normalized.values())
        ids = list(normalized.keys())

        if skip_pending_filter:
            pending_ids = ids
        else:
            pending_ids = await self._filter_pending_ids(table_name, ids)
            skipped = len(ids) - len(pending_ids)
            if skipped:
                self.total_skipped += skipped
            record_list = [normalized[i] for i in pending_ids if i in normalized]

        if not record_list:
            logger.info("writeback_records 跳过: 无待更新记录 (输入 %s 条)", len(ids))
            return 0

        if update_columns is None:
            update_columns = [
                key
                for key in record_list[0].keys()
                if key != id_field and key not in (always_set or {})
            ]

        total_affected = 0
        chunks = [
            record_list[i : i + self.update_chunk_size]
            for i in range(0, len(record_list), self.update_chunk_size)
        ]
        for chunk_index, chunk in enumerate(chunks, start=1):
            sql, params = self._build_records_update_sql(
                table_name,
                chunk,
                update_columns,
                always_set=always_set,
            )
            for attempt in range(1, self.update_max_retries + 1):
                try:
                    async with self.mysql_pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(sql, params)
                            affected = cur.rowcount
                    total_affected += affected
                    logger.info(
                        "writeback_records 写回 %s 条 (分片 %s/%s, 输入 %s)",
                        affected,
                        chunk_index,
                        len(chunks),
                        len(chunk),
                    )
                    break
                except OperationalError as exc:
                    if attempt >= self.update_max_retries:
                        raise
                    await asyncio.sleep(attempt * 2)
                    logger.warning(
                        "writeback_records 重试 %s/%s: %s",
                        attempt,
                        self.update_max_retries,
                        exc,
                    )

        self.total_written += total_affected
        return total_affected

    def _build_records_update_sql(
        self,
        table_name: str,
        records: list[dict],
        update_columns: list[str],
        *,
        always_set: dict[str, Any] | None = None,
    ) -> tuple[str, list]:
        id_field = self.writeback.id_field
        set_parts: list[str] = []
        params: list[Any] = []

        for column in update_columns:
            case_sql = [f"CASE {id_field}"]
            for row in records:
                case_sql.append("WHEN %s THEN %s")
                params.append(row[id_field])
                params.append(row.get(column))
            case_sql.append("END")
            set_parts.append(f"{column} = {' '.join(case_sql)}")

        for key, value in (always_set or {}).items():
            if isinstance(value, str) and value.upper() == "NOW()":
                set_parts.append(f"{key} = NOW()")
            else:
                set_parts.append(f"{key} = %s")
                params.append(value)

        placeholders = ",".join(["%s"] * len(records))
        params.extend(row[id_field] for row in records)
        sql = f"""
            UPDATE {table_name}
            SET {", ".join(set_parts)}
            WHERE {id_field} IN ({placeholders})
              AND ({self.writeback.pending_condition})
        """
        return sql, params


# 兼容旧名称
DirectMysqlWriteback = MysqlWritebackClient


# ---------------------------------------------------------------------------
# 统一入口
# ---------------------------------------------------------------------------


async def run_non_redis_pipeline(
    job: StreamJobConfig,
    process_batch: BatchHandler,
):
    """
    非 Redis 流水线：读取 -> 外部 process_batch -> 由外部决定何时 writeback。

    process_batch 签名::
        async def process_batch(rows, writeback, job) -> None:
            ...
            await writeback.writeback(job.table, success_ids)
    """
    if not job.database:
        raise ValueError("请指定 database")
    if not job.table:
        raise ValueError("请指定 table")

    mysql_dict = mysql_config_to_dict(MYSQL_CFG, job.database)
    reader = MysqlTableReader(mysql_config=mysql_dict, batch_size=job.batch_size)
    writeback = MysqlWritebackClient(mysql_config=mysql_dict, writeback=job.writeback)

    await reader.init_connections()
    await writeback.init_connections()
    try:
        async for batch in reader.iter_batches(job):
            await process_batch(batch, writeback, job)
    finally:
        await reader.close()
        await writeback.close()

    logger.info(
        "非 Redis 流水线完成: 读取 %s 条，写回 %s 条，跳过 %s 条",
        reader.total_read,
        writeback.total_written,
        writeback.total_skipped,
    )


async def run_stream_job(
    job: StreamJobConfig,
    process_batch: BatchHandler | None = None,
):
    if not job.database:
        raise ValueError("请指定 database")
    if not job.table:
        raise ValueError("请指定 table")

    mysql_dict = mysql_config_to_dict(MYSQL_CFG, job.database)

    if not job.use_redis:
        if process_batch is None:
            raise ValueError(
                "非 Redis 模式需提供 process_batch 回调，请使用 run_non_redis_pipeline(job, handler)"
            )
        await run_non_redis_pipeline(job, process_batch)
        return

    if not job.redis.queue_key:
        raise ValueError("Redis 模式需配置 redis.queue_key")

    streamer = MySQLToRedisStreamer(
        mysql_config=mysql_dict,
        redis_settings=job.redis,
        upload_format=job.upload_format,
        batch_size=job.batch_size,
        monitor_interval=job.monitor_interval,
        threshold_ratio=job.threshold_ratio,
    )

    success_monitor = None
    streamer_done = asyncio.Event()
    if job.redis.success_queue_key:
        success_monitor = SuccessQueueMonitor(
            mysql_config=mysql_dict,
            redis_settings=job.redis,
            writeback=job.writeback,
            upload_format=job.upload_format,
            batch_size=job.batch_size,
            monitor_interval=job.monitor_interval,
        )

    monitor_task = None
    if success_monitor is not None:
        monitor_task = asyncio.create_task(
            success_monitor.run_phases(
                job.table,
                streamer_done,
                job.success_queue_stable_seconds,
            )
        )

    upload_error = None
    try:
        await streamer.run_upload_only(job)
    except Exception as exc:
        upload_error = exc
        logger.error("Redis 上传失败: %s", exc)
        streamer.stop()
        if success_monitor is not None:
            success_monitor.stop()
    finally:
        if not streamer_done.is_set():
            streamer_done.set()
        await streamer.close()

    if monitor_task is not None:
        await monitor_task

    if upload_error is not None:
        raise upload_error




def apply_job_overrides(job: StreamJobConfig, args: argparse.Namespace) -> StreamJobConfig:
    """CLI 参数覆盖 profile 中的部分字段。"""
    if args.database:
        job.database = args.database
    if args.table:
        job.table = args.table
    if args.columns:
        job.columns = args.columns
    if args.cursor_field:
        job.cursor_field = args.cursor_field
    if args.conditions is not None:
        job.conditions = args.conditions
    if args.no_redis:
        job.use_redis = False
    if args.redis_host:
        job.redis.host = args.redis_host
    if args.redis_port is not None:
        job.redis.port = args.redis_port
    if args.redis_db is not None:
        job.redis.db = args.redis_db
    if args.redis_password is not None:
        job.redis.password = args.redis_password or None
    if args.queue_key:
        job.redis.queue_key = args.queue_key
    if args.success_queue_key:
        job.redis.success_queue_key = args.success_queue_key
    if args.batch_size is not None:
        job.batch_size = args.batch_size
    if args.monitor_interval is not None:
        job.monitor_interval = args.monitor_interval
    if args.threshold_ratio is not None:
        job.threshold_ratio = args.threshold_ratio
    if args.success_queue_stable_seconds is not None:
        job.success_queue_stable_seconds = args.success_queue_stable_seconds
    return job


def build_arg_parser() -> argparse.ArgumentParser:
    from tools.common_mysql.profile_loader import list_builtin_platforms

    parser = argparse.ArgumentParser(description="MySQL 表数据流式读取（Redis 队列 / 直接写回）")
    platform_group = parser.add_mutually_exclusive_group(required=True)
    platform_group.add_argument(
        "--platform",
        choices=list_builtin_platforms(),
        help=f"内置平台配置（{', '.join(list_builtin_platforms()) or '无'}）",
    )
    platform_group.add_argument(
        "--profile",
        help="自定义 profile JSON 路径（与 --platform 二选一）",
    )

    parser.add_argument("--database", help="覆盖 profile 中的 database_name")
    parser.add_argument("--table", help="覆盖 profile 中的 table_name")
    parser.add_argument("--columns", help="覆盖 profile 中的 columns")
    parser.add_argument("--cursor-field", help="覆盖 profile 中的 cursor_field")
    parser.add_argument("--conditions", help="覆盖 profile 中的 conditions SQL 片段")
    parser.add_argument("--no-redis", action="store_true", help="非 Redis 模式，仅读取并由回调写回")

    parser.add_argument("--redis-host", help="覆盖 Redis host")
    parser.add_argument("--redis-port", type=int, help="覆盖 Redis port")
    parser.add_argument("--redis-db", type=int, help="覆盖 Redis db")
    parser.add_argument("--redis-password", default=None, help="覆盖 Redis password")
    parser.add_argument("--queue-key", help="覆盖 Redis 任务队列 key")
    parser.add_argument("--success-queue-key", help="覆盖 Redis 成功队列 key")

    parser.add_argument("--batch-size", type=int, help="覆盖批次大小")
    parser.add_argument("--monitor-interval", type=int, help="覆盖队列监控间隔（秒）")
    parser.add_argument("--threshold-ratio", type=float, help="覆盖队列阈值比例")
    parser.add_argument(
        "--success-queue-stable-seconds",
        type=int,
        help="覆盖成功队列稳定等待秒数",
    )
    return parser




async def example_non_redis_process_batch(
    rows: list[dict],
    writeback: MysqlWritebackClient,
    job: StreamJobConfig,
) -> None:
    """
    非 Redis 模式示例：此处衔接外部处理逻辑，仅将处理成功的记录写回。

    替换为本业务的真实处理，例如调 API、写文件、跑解析等。
    """
    success_ids: list[str] = []
    id_field = job.writeback.id_field
    for row in rows:
        # TODO: 替换为真实外部处理
        # ok = await your_external_handler(row)
        ok = True
        if ok and row.get(id_field):
            success_ids.append(str(row[id_field]))

    if success_ids:
        affected = await writeback.writeback(job.table, success_ids)
        logger.info("外部处理完成，本批写回 %s 条", affected)


async def run_job(job: StreamJobConfig) -> None:
    logger.info(
        "启动任务: db=%s table=%s redis=%s format=%s",
        job.database,
        job.table,
        job.use_redis,
        job.upload_format,
    )

    if job.use_redis:
        await run_stream_job(job)
    else:
        await run_non_redis_pipeline(job, example_non_redis_process_batch)

    logger.info("任务完成")


def main(argv: list[str] | None = None) -> None:
    try:
        import aiomysql  # noqa: F401
        import redis.asyncio  # noqa: F401
    except ImportError:
        logger.error("请先安装依赖: pip install aiomysql redis")
        raise SystemExit(1)

    from tools.common_mysql.profile_loader import load_profile_data

    parser = build_arg_parser()
    args = parser.parse_args(argv)

    platform_key = resolve_platform_key(args)
    try:
        profile_data = load_profile_data(platform_key)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    job = apply_job_overrides(build_stream_job_config(profile_data), args)
    if not job.database:
        logger.error("profile 未配置 database_name")
        raise SystemExit(1)

    asyncio.run(run_job(job))


if __name__ == "__main__":
    main()
