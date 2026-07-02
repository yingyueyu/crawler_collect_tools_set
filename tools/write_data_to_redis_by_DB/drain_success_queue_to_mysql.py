"""
仅消费 Redis 成功队列，回写 MySQL is_finish=1（本目录旧版入口，已弃用）。

请改用通用独立模块::

    python tools/common_mysql/drain_success_queue_to_mysql.py --platform maven

用于先消化 maven_html:index_success_urls 等积压，不推送新爬取任务。
与 write_data_from_db.py 并行运行 SuccessQueueMonitor 时可能产生锁竞争，请先停掉后者。
"""
from __future__ import annotations

import argparse
import asyncio
import sys

from tools.key_token_config import REDIS_GIT_GET_HTML
from setting import MYSQL_CONFIG
from write_data_from_db import SuccessQueueMonitor, logger


class SuccessQueueDrainer(SuccessQueueMonitor):
    """成功队列专用回写器：更小分片、支持尾批、失败不退出。"""

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
            f"成功队列回写启动 | 队列={self.success_queue_key} | "
            f"表={table_name} | 批次={self.batch_size} | "
            f"UPDATE分片={self.update_chunk_size} | id字段={self.id_field}"
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
                    f"成功队列长度 {queue_length}，开始回写 "
                    f"(本批最多 {min(self.batch_size, queue_length)} 条)"
                )
                try:
                    await self.process_batch(table_name)
                    batches_done += 1
                except Exception as e:
                    logger.error(
                        f"批次回写失败，Redis 队列未删除，将在 {self.monitor_interval}s 后重试: {e}"
                    )

                if max_batches is not None and batches_done >= max_batches:
                    logger.info(f"已处理 {batches_done} 批，达到 --once/--max-batches 限制，退出")
                    break

                await asyncio.sleep(self.monitor_interval)

            logger.info(
                f"回写结束 | 累计新完成 {self.total_processed} 条 | "
                f"累计队列消费 {self.total_queue_consumed} 条 | "
                f"累计跳过 {self.total_skipped} 条"
            )
        finally:
            if self.redis_client is not None:
                from write_data_from_db import close_redis_client

                await close_redis_client(self.redis_client)
                self.redis_client = None
                logger.info("Redis 连接已关闭")

            if self.mysql_pool is not None:
                self.mysql_pool.close()
                await self.mysql_pool.wait_closed()
                self.mysql_pool = None
                logger.info("MySQL 连接池已关闭")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="将 Redis 成功队列回写到 MySQL（仅消费成功队列，不推送任务）"
    )
    parser.add_argument(
        "--table",
        default="maven_purl_html_bill_status",
        help="MySQL 表名",
    )
    parser.add_argument(
        "--success-queue",
        default="maven_html:index_success_urls",
        help="Redis 成功队列 key",
    )
    parser.add_argument(
        "--id-field",
        default="purl",
        help="主键/回写字段：maven 用 purl，npm 用 lower_purl",
    )
    parser.add_argument("--redis-host", default=REDIS_GIT_GET_HTML["host"])
    parser.add_argument("--redis-port", type=int, default=REDIS_GIT_GET_HTML["port"])
    parser.add_argument("--redis-db", type=int, default=REDIS_GIT_GET_HTML["db"])
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="每批从 Redis 读取条数（默认 500，比 1000 更易避免锁等待）",
    )
    parser.add_argument(
        "--update-chunk-size",
        type=int,
        default=500,
        help="每条 UPDATE 语句包含的行数（默认 100）",
    )
    parser.add_argument(
        "--update-max-retries",
        type=int,
        default=5,
        help="单分片 UPDATE 失败重试次数",
    )
    parser.add_argument(
        "--monitor-interval",
        type=int,
        default=2,
        help="批次间隔秒数",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="只处理一批后退出（便于试跑）",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="最多处理批次数，默认不限；--once 等价于 --max-batches 1",
    )
    parser.add_argument(
        "--stop-when-empty",
        action="store_true",
        help="队列清空后退出；默认持续轮询等待新数据",
    )
    return parser.parse_args()


async def main_async(args: argparse.Namespace) -> None:
    if not MYSQL_CONFIG.get("database"):
        logger.error("请在 setting.py 中配置 MySQL 的 database 参数")
        sys.exit(1)

    max_batches = 1 if args.once else args.max_batches

    drainer = SuccessQueueDrainer(
        mysql_config=MYSQL_CONFIG,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        success_queue_key=args.success_queue,
        batch_size=args.batch_size,
        monitor_interval=args.monitor_interval,
        id_field=args.id_field,
    )
    drainer.update_chunk_size = args.update_chunk_size
    drainer.update_max_retries = args.update_max_retries

    try:
        await drainer.run(
            args.table,
            drain_partial=True,
            stop_when_empty=args.stop_when_empty,
            max_batches=max_batches,
        )
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在退出...")
        drainer.stop()


def main() -> None:
    try:
        import aiomysql  # noqa: F401
        import redis.asyncio  # noqa: F401
    except ImportError:
        logger.error("请先安装依赖: pip install aiomysql redis")
        sys.exit(1)

    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
