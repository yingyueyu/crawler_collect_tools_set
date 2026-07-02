import asyncio
import aiomysql
import redis.asyncio as redis
import logging
import json
import time
import sys
from pathlib import Path
from pymysql.err import OperationalError

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.key_token_config import REDIS_GIT_GET_HTML
from setting import MYSQL_CONFIG, MySQLConfig

MYSQL_CFG = MySQLConfig.from_env()


async def create_mysql_pool(mysql_config: dict):
    # 必须 autocommit=True：否则 UPDATE 的 rowcount 有值，但事务未提交，重启后库中无变化
    return await aiomysql.create_pool(
        host=mysql_config["host"],
        port=mysql_config["port"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        db=mysql_config["database"],
        charset=MYSQL_CFG.charset,
        autocommit=True,
        minsize=MYSQL_CFG.minsize,
        maxsize=MYSQL_CFG.maxsize,
        connect_timeout=MYSQL_CFG.connect_timeout,
        pool_recycle=MYSQL_CFG.pool_recycle,
    )


async def redis_call(result):
    """兼容 sync / asyncio Redis：方法返回值可能是 coroutine，也可能是直接结果。"""
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def resolve_id_field(columns: str, id_field: str = None) -> str:
    """解析游标/队列/回写字段名。未指定时：单列 COLUMNS 用该列名，否则默认 lower_purl。"""
    if id_field:
        return id_field
    if columns and columns.strip() != "*":
        parts = [p.strip() for p in columns.split(",") if p.strip()]
        if len(parts) == 1:
            return parts[0]
    return "lower_purl"


class MySQLToRedisStreamer:
    def __init__(
        self,
        mysql_config: dict,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        batch_size: int = 500,
        monitor_interval: int = 5,
        threshold_ratio: float = 0.1
    ):
        self.mysql_config = mysql_config
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.batch_size = batch_size
        self.monitor_interval = monitor_interval
        self.threshold = int(batch_size * threshold_ratio)
        self.redis_client = None
        self.mysql_pool = None
        self.stop_event = asyncio.Event()
        self.last_cursor = ""
        self.cursor_field = "lower_purl"
        self.total_count = 0
        self.uploaded_count = 0
        self.where_clause = ""
        self.where_params = []
        self.start_time = 0
        self.last_log_time = 0

    async def init_connections(self):
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                decode_responses=True,
            )
            await redis_call(self.redis_client.ping())
            logger.info(f"成功连接Redis: {self.redis_host}:{self.redis_port}")

            self.mysql_pool = await create_mysql_pool(self.mysql_config)
            logger.info(f"成功连接MySQL: {self.mysql_config['host']}:{self.mysql_config['port']}")
        except Exception as e:
            logger.error(f"连接初始化失败: {e}")
            raise

    def build_where_clause(self, conditions=None):
        """
        构建WHERE子句
        
        :param conditions: 查询条件，可以是以下格式之一：
            1. 字符串：直接作为WHERE子句，如 "is_finish = 0 AND status = 1"
            2. 字典：键值对形式，如 {"is_finish": 0, "status": 1}，默认使用AND连接
            3. 列表：条件列表，如 [("is_finish", "=", 0), ("status", ">", 1)]
            4. None：不添加WHERE子句
        :return: (where_clause, params)
        """
        if conditions is None:
            return "", []
        
        if isinstance(conditions, str):
            # 直接使用字符串作为WHERE子句
            return conditions, []
        
        elif isinstance(conditions, dict):
            # 字典形式，默认AND连接
            clauses = []
            params = []
            for key, value in conditions.items():
                clauses.append(f"{key} = %s")
                params.append(value)
            if clauses:
                return " AND ".join(clauses), params
            return "", []
        
        elif isinstance(conditions, list):
            # 列表形式，支持自定义操作符
            # 格式: [(field, operator, value), ...]
            clauses = []
            params = []
            for field, operator, value in conditions:
                clauses.append(f"{field} {operator} %s")
                params.append(value)
            if clauses:
                return " AND ".join(clauses), params
            return "", []
        
        else:
            logger.warning(f"不支持的条件格式: {type(conditions)}")
            return "", []

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
        """用 keyset 分页替代 OFFSET，避免 is_finish 变化后漏传/重复传。"""
        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if self.start_time == 0:
                    self.start_time = time.time()
                    self.last_log_time = self.start_time

                refresh_count_every = 10
                batches_since_count = 0

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

                    logger.debug(f"MySQL 读取 {len(rows)} 条，游标至 {rows[-1][self.cursor_field][:60]}...")
                    yield rows
                    self.last_cursor = rows[-1][self.cursor_field]
                    self.uploaded_count += len(rows)
                    batches_since_count += 1

                    if batches_since_count >= refresh_count_every:
                        batches_since_count = 0

                    current_time = time.time()
                    elapsed_time = current_time - self.start_time
                    speed = self.uploaded_count / elapsed_time if elapsed_time > 0 else 0

                    remaining_in_db = await self.get_total_count(table_name, conn=conn)
                    remaining_time = remaining_in_db / speed if speed > 0 else 0

                    if remaining_time < 60:
                        remaining_str = f"{remaining_time:.1f}秒"
                    elif remaining_time < 3600:
                        remaining_str = f"{remaining_time/60:.1f}分钟"
                    else:
                        remaining_str = f"{remaining_time/3600:.2f}小时"

                    speed_str = f"{speed:.2f}条/秒" if speed < 100 else f"{speed/1000:.2f}k条/秒"
                    cursor_preview = self.last_cursor if len(self.last_cursor) <= 60 else self.last_cursor[:60] + "..."
                    logger.info(
                        f"已上传 {self.uploaded_count} 条到任务队列 | "
                        f"DB剩余 is_finish=0: {remaining_in_db} | "
                        f"游标 {self.cursor_field}>{cursor_preview} | "
                        f"速度: {speed_str} 预计剩余约: {remaining_str}"
                    )

    async def upload_batch(self, queue_key: str, data_batch: list):
        if not data_batch:
            return

        try:
            purls = [item[self.cursor_field] for item in data_batch]
            unique_purls = list(dict.fromkeys(purls))
            dup_count = len(purls) - len(unique_purls)

            if unique_purls:
                push_pipe = self.redis_client.pipeline()
                for purl in unique_purls:
                    push_pipe.lpush(queue_key, purl)
                await redis_call(push_pipe.execute())

            msg = f"成功上传 {len(unique_purls)} 条到队列 {queue_key}"
            if dup_count:
                msg += f"（本批去重 {dup_count} 条）"
            logger.info(msg)
        except Exception as e:
            logger.error(f"上传数据失败: {e}")
            raise

    async def monitor_queue(self, queue_key: str) -> int:
        try:
            length = await redis_call(self.redis_client.llen(queue_key))
            logger.debug(f"队列 {queue_key} 当前长度: {length}")
            return length
        except Exception as e:
            logger.error(f"获取队列长度失败: {e}")
            return -1

    async def close(self):
        await close_redis_client(self.redis_client)
        self.redis_client = None
        logger.info("Redis连接已关闭")

        if self.mysql_pool:
            self.mysql_pool.close()
            await self.mysql_pool.wait_closed()
            self.mysql_pool = None
            logger.info("MySQL连接池已关闭")

    async def run(
        self,
        table_name: str,
        queue_key: str,
        columns: str = "*",
        conditions=None,
        id_field: str = None,
    ):
        """兼容旧用法：上传完成后立即关闭连接。"""
        try:
            await self.run_upload_only(
                table_name, queue_key, columns, conditions, id_field
            )
        finally:
            await self.close()

    async def run_upload_only(
        self,
        table_name: str,
        queue_key: str,
        columns: str = "*",
        conditions=None,
        id_field: str = None,
    ):
        """
        仅执行 MySQL -> Redis 任务队列上传，不关闭连接。
        连接关闭由 main 在成功队列收尾完成后统一处理。
        """
        self.cursor_field = resolve_id_field(columns, id_field)
        logger.info(f"游标字段: {self.cursor_field}")
        await self.init_connections()

        self.where_clause, self.where_params = self.build_where_clause(conditions)
        if self.where_clause:
            logger.info(f"查询条件: WHERE {self.where_clause}")

        self.total_count = await self.get_total_count(table_name)
        logger.info(f"表 {table_name} 符合条件的记录共有 {self.total_count} 条")

        if self.total_count == 0:
            logger.warning("没有符合条件的数据，跳过任务队列上传")
            return

        logger.info(
            f"开始读取并上传第一批数据（批次 {self.batch_size}，"
            f"队列阈值 {self.threshold}）..."
        )
        stream_generator = self.stream_query(table_name, columns)

        batch = None
        async for data_batch in stream_generator:
            batch = data_batch
            await self.upload_batch(queue_key, batch)
            break

        if not batch:
            logger.warning("没有数据可上传，跳过任务队列上传")
            return

        async for data_batch in stream_generator:
            while not self.stop_event.is_set():
                queue_length = await self.monitor_queue(queue_key)

                if queue_length <= self.threshold:
                    logger.info(
                        f"队列长度 {queue_length} 已低于阈值 {self.threshold}，开始上传下一批"
                    )
                    break

                await asyncio.sleep(self.monitor_interval)

            if self.stop_event.is_set():
                logger.info("收到停止信号，任务队列上传终止")
                break

            await self.upload_batch(queue_key, data_batch)

        logger.info(f"数据上传完成！共上传 {self.uploaded_count} 条数据")

    def stop(self):
        self.stop_event.set()
        logger.info("任务已停止")


class SuccessQueueMonitor:
    def __init__(
        self,
        mysql_config: dict,
        redis_host: str = REDIS_GIT_GET_HTML["host"],
        redis_port: int = REDIS_GIT_GET_HTML["port"],
        redis_db: int = 2,
        success_queue_key: str = "npm_html:success_urls",
        batch_size: int = 1000,
        monitor_interval: int = 5,
        id_field: str = "lower_purl",
    ):
        self.mysql_config = mysql_config
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.success_queue_key = success_queue_key
        self.batch_size = batch_size
        self.monitor_interval = monitor_interval
        self.id_field = id_field
        self.redis_client = None
        self.mysql_pool = None
        self.stop_event = asyncio.Event()
        self.processed_count = 0
        self.start_time = 0
        self.last_batch_time = 0
        self.total_processed = 0
        self.total_queue_consumed = 0
        self.total_skipped = 0
        self.update_chunk_size = 500
        self.update_max_retries = 3

    async def init_connections(self):
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                decode_responses=True,
            )
            await redis_call(self.redis_client.ping())
            logger.info(f"成功队列监控器连接Redis: {self.redis_host}:{self.redis_port}")

            self.mysql_pool = await create_mysql_pool(self.mysql_config)
            logger.info(f"成功队列监控器连接MySQL: {self.mysql_config['host']}:{self.mysql_config['port']}")
        except Exception as e:
            logger.error(f"成功队列监控器连接初始化失败: {e}")
            raise

    async def get_queue_length(self) -> int:
        try:
            length = await redis_call(self.redis_client.llen(self.success_queue_key))
            logger.debug(f"成功队列 {self.success_queue_key} 当前长度: {length}")
            return length
        except Exception as e:
            logger.error(f"获取成功队列长度失败: {e}")
            return -1

    async def fetch_batch_from_queue(self) -> list:
        try:
            items = await redis_call(
                self.redis_client.lrange(
                    self.success_queue_key, 0, self.batch_size - 1
                )
            )
            return items
        except Exception as e:
            logger.error(f"从队列获取数据失败: {e}")
            return []

    async def remove_from_queue(self, count: int):
        try:
            await redis_call(self.redis_client.ltrim(self.success_queue_key, count, -1))
            logger.debug(f"已从队列删除 {count} 条记录")
        except Exception as e:
            logger.error(f"从队列删除数据失败: {e}")
            raise

    async def _filter_pending_ids(self, table_name: str, ids: list) -> list:
        """只保留库中存在且 is_finish=0 的 id，跳过已完成/不存在的。"""
        if not ids:
            return []

        pending = []
        chunks = [
            ids[i:i + self.update_chunk_size]
            for i in range(0, len(ids), self.update_chunk_size)
        ]
        for chunk in chunks:
            placeholders = ",".join(["%s"] * len(chunk))
            query = (
                f"SELECT {self.id_field} FROM {table_name} "
                f"WHERE {self.id_field} IN ({placeholders}) AND is_finish = 0"
            )
            async with self.mysql_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, tuple(chunk))
                    rows = await cur.fetchall()
                    pending.extend(row[0] for row in rows)
        return pending

    async def _execute_update_chunk(self, table_name: str, ids: list) -> int:
        placeholders = ",".join(["%s"] * len(ids))
        update_query = f"""
            UPDATE {table_name}
            SET is_finish = 1, updated_time = NOW()
            WHERE {self.id_field} IN ({placeholders})
              AND is_finish = 0
        """
        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(update_query, tuple(ids))
                return cur.rowcount

    async def update_database(self, table_name: str, ids: list) -> tuple:
        if not ids:
            return 0, [], []

        pending_ids = await self._filter_pending_ids(table_name, ids)
        skipped_ids = [purl for purl in ids if purl not in set(pending_ids)]
        skipped = len(skipped_ids)
        if skipped:
            self.total_skipped += skipped
            sample_raw = ids[:2]
            sample_norm = [self.normalize_id(x) for x in sample_raw]
            logger.info(
                f"预过滤: 去重后 {len(ids)} 条 | 待更新 {len(pending_ids)} 条 | "
                f"跳过 {skipped} 条（已完成或不在表中）| 样例 {sample_raw} -> {sample_norm}"
            )
        if not pending_ids:
            return 0, [], skipped_ids

        total_affected = 0
        chunks = [
            pending_ids[i:i + self.update_chunk_size]
            for i in range(0, len(pending_ids), self.update_chunk_size)
        ]

        for chunk_index, chunk in enumerate(chunks, start=1):
            for attempt in range(1, self.update_max_retries + 1):
                try:
                    affected_rows = await self._execute_update_chunk(table_name, chunk)
                    total_affected += affected_rows
                    logger.info(
                        f"新完成 {affected_rows} 条 "
                        f"(分片 {chunk_index}/{len(chunks)}, 本片 {len(chunk)} 条)"
                    )
                    break
                except OperationalError as e:
                    if attempt >= self.update_max_retries:
                        logger.error(
                            f"更新数据库失败，已重试 {self.update_max_retries} 次，"
                            f"保留 Redis 队列待下次重试: {e}"
                        )
                        raise
                    wait_seconds = attempt * 2
                    logger.warning(
                        f"更新数据库失败(分片 {chunk_index}/{len(chunks)}，"
                        f"第 {attempt}/{self.update_max_retries} 次): {e}，"
                        f"{wait_seconds}s 后重试"
                    )
                    await asyncio.sleep(wait_seconds)

        self.processed_count += total_affected
        if total_affected == 0:
            logger.warning(
                f"本批次无新完成: 预过滤后 {len(pending_ids)} 条仍无法更新，"
                f"样例 {pending_ids[:3]}"
            )
        elif total_affected < len(pending_ids):
            logger.warning(
                f"部分未新完成: 预过滤后 {len(pending_ids)} 条，实际新完成 {total_affected} 条"
            )
        return total_affected, pending_ids, skipped_ids

    def normalize_id(self, value: str) -> str:
        """队列值归一化为 DB 主键字段格式。"""
        import re
        from urllib.parse import unquote

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
    
    async def process_batch(self, table_name: str):
        items = await self.fetch_batch_from_queue()
        
        if not items:
            return
        
        # 记录开始时间
        if self.start_time == 0:
            self.start_time = time.time()
            self.last_batch_time = self.start_time
        
        # 将 URL 转换为 purl 格式（假设数据库中存储的是 purl）
        ids = []
        for item in items:
            if item:
                purl = self.normalize_id(item)
                ids.append(purl)
                logger.debug(f"转换 URL 为 purl: {item} -> {purl}")
            else:
                logger.warning("队列中存在空数据")
        
        if ids:
            queue_count = len(items)
            unique_ids = list(dict.fromkeys(ids))
            dup_count = len(ids) - len(unique_ids)

            current_time = time.time()
            batch_duration = current_time - self.last_batch_time
            self.last_batch_time = current_time

            newly_completed, pending_ids, skipped_ids = await self.update_database(
                table_name, unique_ids
            )
            await self.remove_from_queue(queue_count)

            self.total_queue_consumed += queue_count
            self.total_processed += newly_completed
            total_elapsed = current_time - self.start_time
            avg_speed = (
                self.total_processed / total_elapsed if total_elapsed > 0 else 0
            )
            avg_speed_str = (
                f"{avg_speed:.2f}条/秒" if avg_speed < 100 else f"{avg_speed/1000:.2f}k条/秒"
            )
            dup_part = f" | 重复 {dup_count}" if dup_count else ""
            logger.info(
                f"处理批次: 队列消费 {queue_count} 条 | 去重 {len(unique_ids)} 条"
                f"{dup_part} | 新完成 {newly_completed} 条 | "
                f"平均新完成速度: {avg_speed_str} | "
                f"累计队列消费: {self.total_queue_consumed} | "
                f"累计新完成: {self.total_processed} | "
                f"累计跳过: {self.total_skipped} 条"
            )

    async def _monitor_once(self, table_name: str, *, require_threshold: bool = True):
        queue_length = await self.get_queue_length()
        if queue_length <= 0:
            return

        if require_threshold and queue_length < self.batch_size:
            logger.debug(
                f"成功队列长度 {queue_length}，等待达到阈值 {self.batch_size}"
            )
            return

        if require_threshold:
            logger.info(
                f"成功队列长度 {queue_length} 达到阈值 {self.batch_size}，开始处理"
            )
        try:
            await self.process_batch(table_name)
        except Exception as e:
            logger.error(
                f"批次处理失败，Redis 成功队列未删除，将在下次轮询重试: {e}"
            )

    async def _wait_queue_stable(self, stable_seconds: int) -> int:
        """成功队列在 stable_seconds 内无增长则视为稳定。"""
        last_length = await self.get_queue_length()
        stable_since = time.time()
        logger.info(
            f"等待成功队列 {stable_seconds}s 无增长（当前 {last_length} 条）..."
        )

        while not self.stop_event.is_set():
            await asyncio.sleep(1)
            current_length = await self.get_queue_length()
            if current_length < 0:
                continue

            if current_length > last_length:
                logger.info(
                    f"成功队列增长 {last_length} -> {current_length}，重置稳定计时"
                )
                last_length = current_length
                stable_since = time.time()
            else:
                last_length = current_length
                elapsed = time.time() - stable_since
                if elapsed >= stable_seconds:
                    logger.info(
                        f"成功队列已连续 {stable_seconds}s 无增长，当前 {current_length} 条"
                    )
                    return current_length

        return last_length

    async def _drain_all(self, table_name: str):
        """忽略批次阈值，将成功队列剩余数据全部回写 MySQL。"""
        while not self.stop_event.is_set():
            queue_length = await self.get_queue_length()
            if queue_length <= 0:
                logger.info("成功队列已排空")
                break

            logger.info(
                f"收尾回写: 成功队列剩余 {queue_length} 条"
                f"（忽略批次阈值 {self.batch_size}）"
            )
            try:
                await self.process_batch(table_name)
            except Exception as e:
                logger.error(f"收尾回写失败，稍后重试: {e}")
                await asyncio.sleep(self.monitor_interval)

    async def close(self):
        await close_redis_client(self.redis_client)
        self.redis_client = None
        logger.info("成功队列监控器Redis连接已关闭")

        if self.mysql_pool:
            self.mysql_pool.close()
            await self.mysql_pool.wait_closed()
            self.mysql_pool = None
            logger.info("成功队列监控器MySQL连接池已关闭")

    async def run_phases(
        self,
        table_name: str,
        streamer_done: asyncio.Event,
        stable_seconds: int = 30,
    ):
        """
        分阶段运行成功队列监控：
        1. 任务队列上传期间，按批次阈值回写
        2. 上传完成后等待成功队列 stable_seconds 无增长
        3. 忽略阈值，将全部剩余成功队列回写 MySQL
        """
        await self.init_connections()
        logger.info(
            f"成功队列监控器启动，监控队列: {self.success_queue_key}，"
            f"批次大小: {self.batch_size}，稳定等待: {stable_seconds}s"
        )

        try:
            while not streamer_done.is_set() and not self.stop_event.is_set():
                await self._monitor_once(table_name, require_threshold=True)
                await asyncio.sleep(self.monitor_interval)

            if self.stop_event.is_set():
                return

            logger.info("任务队列上传已完成，进入成功队列稳定等待阶段")
            await self._wait_queue_stable(stable_seconds)

            if self.stop_event.is_set():
                return

            await self._drain_all(table_name)

            logger.info(
                f"成功队列收尾完成，累计新完成 {self.total_processed} 条，"
                f"累计队列消费 {self.total_queue_consumed} 条，"
                f"累计跳过 {self.total_skipped} 条"
            )
        finally:
            await self.close()

    async def run(self, table_name: str):
        """兼容旧用法：持续监控直到收到 stop 信号。"""
        await self.init_connections()

        logger.info(
            f"成功队列监控器启动，监控队列: {self.success_queue_key}，"
            f"批次大小: {self.batch_size}"
        )

        try:
            while not self.stop_event.is_set():
                await self._monitor_once(table_name, require_threshold=True)
                await asyncio.sleep(self.monitor_interval)

            logger.info(
                f"成功队列监控器停止，累计新完成 {self.processed_count} 条，"
                f"累计队列消费 {self.total_queue_consumed} 条，"
                f"累计跳过 {self.total_skipped} 条"
            )
        finally:
            await self.close()

    def stop(self):
        self.stop_event.set()
        logger.info("成功队列监控器收到停止信号")


async def main():
    # 配置参数
    TABLE_NAME = "golang_purl_html_list_status"
    # TABLE_NAME = "github_product_classify_monthly_new_add"  # 替换为实际表名
    # TABLE_NAME = "npm_purl_html_bill"  # 替换为实际表名
    # QUEUE_KEY = "npm_queue_minor"        # 替换为实际队列名（任务队列）
    # SUCCESS_QUEUE_KEY = "npm_success_urls"  # 成功队列名
    
    # QUEUE_KEY = "npm_html:urls"  # 替换为实际队列名（任务队列）
    # SUCCESS_QUEUE_KEY = "npm_html:success_urls"  # 成功队列名

    # COLUMNS = "lower_purl"  # 需要查询的列
    # ID_FIELD = "lower_purl"  # 游标/队列/回写字段：npm 用 lower_purl，maven 用 purl
    


    # QUEUE_KEY = "maven_html:urls"  # 替换为实际队列名（任务队列）
    # SUCCESS_QUEUE_KEY = "maven_html:index_success_urls"  # 成功队列名

    QUEUE_KEY = "go_html:urls"  # 替换为实际队列名（任务队列）
    SUCCESS_QUEUE_KEY = "go_html:success_urls"  # 成功队列名

    # QUEUE_KEY = "github_html:urls"  # 替换为实际队列名（任务队列）
    # SUCCESS_QUEUE_KEY = "github_html:success_urls"  # 成功队列名


    COLUMNS = "purl_name"  # 需要查询的列
    ID_FIELD = "purl_name"  # 游标/队列/回写字段：npm 用 lower_purl，maven 用 purl


    # 查询条件配置（三种方式任选其一）
    # 方式1: 字符串形式（直接写WHERE子句，注意SQL注入风险）
    CONDITIONS = "is_finish = 0"
    # CONDITIONS = "updated_time < '2026-06-25 09:00:00'"
    
    
    # 方式2: 字典形式（默认使用=和AND连接）
    # CONDITIONS = {"is_finish": 0, "status": 1}
    
    # 方式3: 列表形式（支持自定义操作符）
    # CONDITIONS = [("is_finish", "=", 0), ("created_time", ">", "2024-01-01")]
    
    # 方式4: None（无条件查询）
    # CONDITIONS = None
    
    # Redis配置
    REDIS_HOST = REDIS_GIT_GET_HTML["host"]
    REDIS_PORT = REDIS_GIT_GET_HTML["port"]
    REDIS_DB = REDIS_GIT_GET_HTML["db"]
    
    # 批次和监控配置
    BATCH_SIZE = 1000
    MONITOR_INTERVAL = 2  # 秒
    THRESHOLD_RATIO = 0.2  # 阈值比例
    SUCCESS_QUEUE_STABLE_SECONDS = 30  # 任务队列上传完成后，成功队列无增长等待秒数

    streamer = None
    success_monitor = None
    streamer_done = asyncio.Event()

    if TABLE_NAME and QUEUE_KEY:
        streamer = MySQLToRedisStreamer(
            mysql_config=MYSQL_CONFIG,
            redis_host=REDIS_HOST,
            redis_port=REDIS_PORT,
            redis_db=REDIS_DB,
            batch_size=BATCH_SIZE,
            monitor_interval=MONITOR_INTERVAL,
            threshold_ratio=THRESHOLD_RATIO,
        )
    else:
        streamer_done.set()

    if TABLE_NAME and SUCCESS_QUEUE_KEY:
        success_monitor = SuccessQueueMonitor(
            mysql_config=MYSQL_CONFIG,
            redis_host=REDIS_HOST,
            redis_port=REDIS_PORT,
            redis_db=REDIS_DB,
            success_queue_key=SUCCESS_QUEUE_KEY,
            batch_size=BATCH_SIZE,
            monitor_interval=MONITOR_INTERVAL,
            id_field=ID_FIELD,
        )

    if streamer is None and success_monitor is None:
        logger.error("请配置 TABLE_NAME、QUEUE_KEY 和 SUCCESS_QUEUE_KEY")
        return

    logger.info("启动所有任务...")

    monitor_task = None
    if success_monitor is not None:
        monitor_task = asyncio.create_task(
            success_monitor.run_phases(
                TABLE_NAME,
                streamer_done,
                SUCCESS_QUEUE_STABLE_SECONDS,
            )
        )

    upload_error = None
    try:
        if streamer is not None:
            await streamer.run_upload_only(
                TABLE_NAME, QUEUE_KEY, COLUMNS, CONDITIONS, ID_FIELD
            )
    except KeyboardInterrupt:
        logger.info("收到键盘中断信号")
        if streamer is not None:
            streamer.stop()
        if success_monitor is not None:
            success_monitor.stop()
        raise
    except Exception as e:
        upload_error = e
        logger.error(f"任务队列上传失败: {e}")
        if streamer is not None:
            streamer.stop()
    finally:
        if not streamer_done.is_set():
            streamer_done.set()
            logger.info(
                f"任务队列上传阶段结束，等待成功队列 "
                f"{SUCCESS_QUEUE_STABLE_SECONDS}s 无增长后收尾回写..."
            )

    if monitor_task is not None:
        await monitor_task

    if streamer is not None:
        await streamer.close()

    if upload_error is not None:
        raise upload_error

    logger.info("全部任务完成，进程退出")


if __name__ == "__main__":
    try:
        import aiomysql
        import redis.asyncio
    except ImportError:
        logger.error("请先安装依赖: pip install aiomysql redis")
        exit(1)
    
    if not MYSQL_CONFIG.get("database"):
        logger.error("请在setting.py中配置MySQL的database参数")
        exit(1)
    
    asyncio.run(main())