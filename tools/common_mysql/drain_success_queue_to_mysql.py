"""
仅消费 Redis 成功队列，回写 MySQL（独立入口，不依赖 write_data_from_db.py）。

与 write_data_from_db.run_stream_job 并行运行 SuccessQueueMonitor 时可能产生锁竞争，
请先停掉后者。

用法::

    python tools/common_mysql/drain_success_queue_to_mysql.py --platform maven

    python tools/common_mysql/drain_success_queue_to_mysql.py --platform maven --once
    python tools/common_mysql/drain_success_queue_to_mysql.py --profile path/to/custom.json \\
        --batch-size 500 --update-chunk-size 100 --stop-when-empty
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tools.common_mysql.profile_loader import load_profile_data
from tools.common_mysql.stream_job_config import (
    StreamJobConfig,
    build_stream_job_config,
    logger,
    resolve_platform_key,
)
from tools.common_mysql.success_queue import run_drain_success_queue


def build_drain_arg_parser() -> argparse.ArgumentParser:
    from tools.common_mysql.profile_loader import list_builtin_platforms

    parser = argparse.ArgumentParser(
        description="仅消费 Redis 成功队列并回写 MySQL（不推送新任务）"
    )
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
    parser.add_argument("--success-queue-key", help="覆盖 Redis 成功队列 key")

    parser.add_argument("--redis-host", help="覆盖 Redis host")
    parser.add_argument("--redis-port", type=int, help="覆盖 Redis port")
    parser.add_argument("--redis-db", type=int, help="覆盖 Redis db")
    parser.add_argument("--redis-password", default=None, help="覆盖 Redis password")

    parser.add_argument("--batch-size", type=int, help="每批从 Redis 读取条数")
    parser.add_argument("--monitor-interval", type=int, help="批次间隔（秒）")
    parser.add_argument(
        "--update-chunk-size",
        type=int,
        default=500,
        help="每条 UPDATE 语句包含的行数（默认 500）",
    )
    parser.add_argument(
        "--update-max-retries",
        type=int,
        default=5,
        help="单分片 UPDATE 失败重试次数（默认 5）",
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
        help="最多处理批次数；--once 等价于 --max-batches 1",
    )
    parser.add_argument(
        "--stop-when-empty",
        action="store_true",
        help="队列清空后退出；默认持续轮询等待新数据",
    )
    parser.add_argument(
        "--no-drain-partial",
        action="store_true",
        help="必须凑满 batch_size 才处理（默认不足一批也会处理尾批）",
    )
    return parser


def apply_drain_overrides(job: StreamJobConfig, args: argparse.Namespace) -> StreamJobConfig:
    if args.database:
        job.database = args.database
    if args.table:
        job.table = args.table
    if args.redis_host:
        job.redis.host = args.redis_host
    if args.redis_port is not None:
        job.redis.port = args.redis_port
    if args.redis_db is not None:
        job.redis.db = args.redis_db
    if args.redis_password is not None:
        job.redis.password = args.redis_password or None
    if args.success_queue_key:
        job.redis.success_queue_key = args.success_queue_key
    if args.batch_size is not None:
        job.batch_size = args.batch_size
    if args.monitor_interval is not None:
        job.monitor_interval = args.monitor_interval
    return job


def main(argv: list[str] | None = None) -> None:
    try:
        import aiomysql  # noqa: F401
        import redis.asyncio  # noqa: F401
    except ImportError:
        logger.error("请先安装依赖: pip install aiomysql redis")
        raise SystemExit(1)

    parser = build_drain_arg_parser()
    args = parser.parse_args(argv)

    platform_key = resolve_platform_key(args)
    try:
        profile_data = load_profile_data(platform_key)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    job = apply_drain_overrides(build_stream_job_config(profile_data), args)
    if not job.database:
        logger.error("profile 未配置 database_name")
        raise SystemExit(1)

    max_batches = 1 if args.once else args.max_batches
    asyncio.run(
        run_drain_success_queue(
            job,
            drain_partial=not args.no_drain_partial,
            stop_when_empty=args.stop_when_empty,
            max_batches=max_batches,
            update_chunk_size=args.update_chunk_size,
            update_max_retries=args.update_max_retries,
        )
    )


if __name__ == "__main__":
    main()

