"""Redis 成功队列消费与 MySQL 写回（独立于 MySQL->Redis 上传流水线）。"""

from __future__ import annotations

import asyncio
import time

import redis.asyncio as redis
from pymysql.err import OperationalError

from tools.common_mysql.stream_job_config import (
    MYSQL_CFG,
    StreamJobConfig,
    UploadFormatConfig,
    WritebackConfig,
    build_update_set_clause,
    close_redis_client,
    create_mysql_pool,
    extract_id_from_queue_item,
    logger,
    mysql_config_to_dict,
    redis_call,
)
from tools.common_mysql.config import MySQLConfig


class SuccessQueueMonitor:
    def __init__(
        self,
        mysql_config: dict,
        redis_settings,
        writeback: WritebackConfig,
        upload_format: UploadFormatConfig | None = None,
        batch_size: int = 1000,
        monitor_interval: int = 5,
        mysql_cfg: MySQLConfig | None = None,
    ):
        self.mysql_config = mysql_config
        self.redis_settings = redis_settings
        self.writeback = writeback
        self.upload_format = upload_format or UploadFormatConfig()
        self.batch_size = batch_size
        self.monitor_interval = monitor_interval
        self.mysql_cfg = mysql_cfg or MYSQL_CFG
        self.redis_client = None
        self.mysql_pool = None
        self.stop_event = asyncio.Event()
        self.total_processed = 0
        self.total_queue_consumed = 0
        self.total_skipped = 0
        self.update_chunk_size = 500
        self.update_max_retries = 3

    @property
    def id_field(self) -> str:
        return self.writeback.id_field

    @property
    def success_queue_key(self) -> str:
        return self.redis_settings.success_queue_key

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
        self.mysql_pool = await create_mysql_pool(self.mysql_config, self.mysql_cfg)
        logger.info("成功队列监控器已连接 Redis / MySQL")

    async def get_queue_length(self) -> int:
        try:
            return await redis_call(self.redis_client.llen(self.success_queue_key))
        except Exception as exc:
            logger.error("获取成功队列长度失败: %s", exc)
            return -1

    async def fetch_batch_from_queue(self) -> list:
        try:
            return await redis_call(
                self.redis_client.lrange(self.success_queue_key, 0, self.batch_size - 1)
            )
        except Exception as exc:
            logger.error("从队列获取数据失败: %s", exc)
            return []

    async def remove_from_queue(self, count: int):
        await redis_call(self.redis_client.ltrim(self.success_queue_key, count, -1))

    async def _filter_pending_ids(self, table_name: str, ids: list) -> list:
        if not ids:
            return []

        pending = []
        chunks = [
            ids[i : i + self.update_chunk_size]
            for i in range(0, len(ids), self.update_chunk_size)
        ]
        for chunk in chunks:
            placeholders = ",".join(["%s"] * len(chunk))
            query = (
                f"SELECT {self.id_field} FROM {table_name} "
                f"WHERE {self.id_field} IN ({placeholders}) "
                f"AND ({self.writeback.pending_condition})"
            )
            async with self.mysql_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, tuple(chunk))
                    rows = await cur.fetchall()
                    pending.extend(row[0] for row in rows)
        return pending

    async def _execute_update_chunk(self, table_name: str, ids: list) -> int:
        placeholders = ",".join(["%s"] * len(ids))
        set_clause, set_params = build_update_set_clause(self.writeback.update_fields)
        update_query = f"""
            UPDATE {table_name}
            SET {set_clause}
            WHERE {self.id_field} IN ({placeholders})
              AND ({self.writeback.pending_condition})
        """
        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(update_query, tuple(set_params) + tuple(ids))
                return cur.rowcount

    async def update_database(self, table_name: str, ids: list) -> tuple[int, list, list]:
        if not ids:
            return 0, [], []

        pending_ids = await self._filter_pending_ids(table_name, ids)
        skipped_ids = [item for item in ids if item not in set(pending_ids)]
        if skipped_ids:
            self.total_skipped += len(skipped_ids)
        if not pending_ids:
            return 0, [], skipped_ids

        total_affected = 0
        chunks = [
            pending_ids[i : i + self.update_chunk_size]
            for i in range(0, len(pending_ids), self.update_chunk_size)
        ]
        for chunk_index, chunk in enumerate(chunks, start=1):
            for attempt in range(1, self.update_max_retries + 1):
                try:
                    affected_rows = await self._execute_update_chunk(table_name, chunk)
                    total_affected += affected_rows
                    logger.info(
                        "写回 %s 条 (分片 %s/%s)",
                        affected_rows,
                        chunk_index,
                        len(chunks),
                    )
                    break
                except OperationalError as exc:
                    if attempt >= self.update_max_retries:
                        raise
                    await asyncio.sleep(attempt * 2)
                    logger.warning("写回重试 %s/%s: %s", attempt, self.update_max_retries, exc)

        self.total_processed += total_affected
        return total_affected, pending_ids, skipped_ids

    async def process_batch(self, table_name: str):
        items = await self.fetch_batch_from_queue()
        if not items:
            return

        ids = []
        for item in items:
            if item:
                ids.append(
                    extract_id_from_queue_item(item, self.id_field, self.upload_format)
                )

        if not ids:
            return

        unique_ids = list(dict.fromkeys(ids))
        newly_completed, _pending_ids, _skipped_ids = await self.update_database(
            table_name, unique_ids
        )
        await self.remove_from_queue(len(items))
        self.total_queue_consumed += len(items)
        logger.info(
            "成功队列批次: 消费 %s | 去重 %s | 写回 %s | 累计写回 %s",
            len(items),
            len(unique_ids),
            newly_completed,
            self.total_processed,
        )

    async def _monitor_once(self, table_name: str, *, require_threshold: bool = True):
        queue_length = await self.get_queue_length()
        if queue_length <= 0:
            return
        if require_threshold and queue_length < self.batch_size:
            return
        await self.process_batch(table_name)

    async def _wait_queue_stable(self, stable_seconds: int) -> int:
        last_length = await self.get_queue_length()
        stable_since = time.time()
        while not self.stop_event.is_set():
            await asyncio.sleep(1)
            current_length = await self.get_queue_length()
            if current_length < 0:
                continue
            if current_length > last_length:
                last_length = current_length
                stable_since = time.time()
            elif time.time() - stable_since >= stable_seconds:
                return current_length
        return last_length

    async def _drain_all(self, table_name: str):
        while not self.stop_event.is_set():
            if await self.get_queue_length() <= 0:
                break
            await self.process_batch(table_name)
            await asyncio.sleep(self.monitor_interval)

    async def close(self):
        await close_redis_client(self.redis_client)
        self.redis_client = None
        if self.mysql_pool:
            self.mysql_pool.close()
            await self.mysql_pool.wait_closed()
            self.mysql_pool = None

    async def run_phases(
        self,
        table_name: str,
        streamer_done: asyncio.Event,
        stable_seconds: int = 30,
    ):
        await self.init_connections()
        try:
            while not streamer_done.is_set() and not self.stop_event.is_set():
                await self._monitor_once(table_name, require_threshold=True)
                await asyncio.sleep(self.monitor_interval)

            if self.stop_event.is_set():
                return

            await self._wait_queue_stable(stable_seconds)
            await self._drain_all(table_name)
            logger.info(
                "成功队列收尾完成，累计写回 %s，消费 %s，跳过 %s",
                self.total_processed,
                self.total_queue_consumed,
                self.total_skipped,
            )
        finally:
            await self.close()

    def stop(self):
        self.stop_event.set()


class SuccessQueueDrainer(SuccessQueueMonitor):
    """
    成功队列专用回写器：仅消费 Redis 成功队列并写回 MySQL。

    可与 write_data_from_db.run_stream_job 并行，但可能产生锁竞争，建议二选一。
    """

    async def run(
        self,
        table_name: str,
        *,
        drain_partial: bool = True,
        stop_when_empty: bool = False,
        max_batches: int | None = None,
    ) -> None:
        await self.init_connections()

        logger.info(
            "成功队列回写启动 | 队列=%s | 表=%s | 批次=%s | UPDATE分片=%s | id字段=%s",
            self.success_queue_key,
            table_name,
            self.batch_size,
            self.update_chunk_size,
            self.id_field,
        )

        batches_done = 0

        try:
            while not self.stop_event.is_set():
                queue_length = await self.get_queue_length()
                if queue_length < 0:
                    await asyncio.sleep(self.monitor_interval)
                    continue

                if queue_length == 0:
                    if stop_when_empty:
                        logger.info("成功队列已清空，任务结束")
                        break
                    await asyncio.sleep(self.monitor_interval)
                    continue

                should_process = queue_length >= self.batch_size or (
                    drain_partial and queue_length > 0
                )
                if not should_process:
                    await asyncio.sleep(self.monitor_interval)
                    continue

                logger.info(
                    "成功队列长度 %s，开始回写 (本批最多 %s 条)",
                    queue_length,
                    min(self.batch_size, queue_length),
                )
                try:
                    await self.process_batch(table_name)
                    batches_done += 1
                except Exception as exc:
                    logger.error(
                        "批次回写失败，Redis 队列未删除，将在 %ss 后重试: %s",
                        self.monitor_interval,
                        exc,
                    )

                if max_batches is not None and batches_done >= max_batches:
                    logger.info(
                        "已处理 %s 批，达到 max_batches 限制，退出",
                        batches_done,
                    )
                    break

                await asyncio.sleep(self.monitor_interval)

            logger.info(
                "回写结束 | 累计写回 %s | 累计队列消费 %s | 累计跳过 %s",
                self.total_processed,
                self.total_queue_consumed,
                self.total_skipped,
            )
        finally:
            await self.close()


async def run_drain_success_queue(
    job: StreamJobConfig,
    *,
    drain_partial: bool = True,
    stop_when_empty: bool = False,
    max_batches: int | None = None,
    update_chunk_size: int = 500,
    update_max_retries: int = 5,
) -> None:
    """仅消费 Redis 成功队列并回写 MySQL，不推送新爬取任务。"""
    if not job.database:
        raise ValueError("请指定 database")
    if not job.table:
        raise ValueError("请指定 table")
    if not job.redis.success_queue_key:
        raise ValueError("请配置 redis.success_queue_key")

    mysql_dict = mysql_config_to_dict(MYSQL_CFG, job.database)
    drainer = SuccessQueueDrainer(
        mysql_config=mysql_dict,
        redis_settings=job.redis,
        writeback=job.writeback,
        upload_format=job.upload_format,
        batch_size=job.batch_size,
        monitor_interval=job.monitor_interval,
    )
    drainer.update_chunk_size = update_chunk_size
    drainer.update_max_retries = update_max_retries

    try:
        await drainer.run(
            job.table,
            drain_partial=drain_partial,
            stop_when_empty=stop_when_empty,
            max_batches=max_batches,
        )
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在退出...")
        drainer.stop()
