"""MinIO 客户端：带连接池/超时/有限重试，供 Scrapy pipeline 在线程池中使用。"""
import io
import random
import threading
import time
from dataclasses import dataclass

import urllib3
from minio import Minio
from minio.error import S3Error

from tools.key_token_config import MINIO_61_TEST
from .utils.logs import get_default_logger

configs_test = MINIO_61_TEST

_log = get_default_logger(name="minio_async", log_dir="app_logs", max_file_mb=50)

# 权限/参数类错误，重试无意义
_NON_RETRYABLE_S3_CODES = frozenset(
    {
        "AccessDenied",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "InvalidBucketName",
        "InvalidObjectName",
        "EntityTooLarge",
        "XMinioInvalidObjectName",
    }
)

# 网络/服务端临时错误，可重试
_RETRYABLE_S3_CODES = frozenset(
    {
        "SlowDown",
        "ServiceUnavailable",
        "InternalError",
        "RequestTimeout",
        "OperationTimedOut",
        "XAmzContentSHA256Mismatch",
    }
)


@dataclass
class PutResult:
    ok: bool
    error_code: str = ""
    error_message: str = ""
    attempts: int = 0

    @property
    def summary(self) -> str:
        if self.ok:
            return "ok"
        if self.error_code:
            return f"{self.error_code}: {self.error_message}".strip(": ")
        return self.error_message or "写入失败"


@dataclass
class MinIOConfig:
    service: str
    access_key: str
    secret_key: str
    secure: bool = False
    pool_maxsize: int = 64
    connect_timeout: int = 15
    read_timeout: int = 30
    urllib3_retries: int = 2


def _safe_log_text(text: str, limit: int = 200) -> str:
    """避免 MinIO 异常信息里的花括号触发 Loguru 二次格式化错误。"""
    return str(text).replace("{", "{{").replace("}", "}}")[:limit]


def _classify_error(exc: Exception) -> tuple[str, str, bool]:
    """返回 (error_code, error_message, retryable)。"""
    if isinstance(exc, S3Error):
        code = exc.code or type(exc).__name__
        msg = exc.message or str(exc)
        if code in _NON_RETRYABLE_S3_CODES:
            return code, msg, False
        if code in _RETRYABLE_S3_CODES:
            return code, msg, True
        if code == "NoSuchBucket":
            return code, msg, False
        # 其它 S3 错误默认不重试，避免 AccessDenied 类被反复打
        return code, msg, False

    if isinstance(exc, (ConnectionError, OSError, TimeoutError)):
        return type(exc).__name__, str(exc), True

    return type(exc).__name__, str(exc), True


class ResilientMinIOClient:
    """
    阻塞 MinIO SDK 的 resilient 包装。
    Scrapy pipeline 通过 twisted.internet.threads.deferToThread 调用，
    避免 put_object 卡住 Twisted/asyncio 事件循环。
    """

    def __init__(self, config: MinIOConfig):
        self.service = config.service
        self.config = config
        self._bucket_lock = threading.Lock()
        self._known_buckets: set[str] = set()
        http_client = urllib3.PoolManager(
            maxsize=config.pool_maxsize,
            timeout=urllib3.Timeout(
                connect=config.connect_timeout,
                read=config.read_timeout,
            ),
            retries=urllib3.Retry(
                total=config.urllib3_retries,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504],
            ),
        )
        print('111,',config)
        self.client = Minio(
            config.service,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
            http_client=http_client,
        )

    def ensure_bucket(self, bucket_name: str) -> PutResult:
        with self._bucket_lock:
            if bucket_name in self._known_buckets:
                return PutResult(ok=True)

            try:
                if not self.client.bucket_exists(bucket_name):
                    self.client.make_bucket(bucket_name)
                    _log.info(f"MinIO 已创建桶: {bucket_name}")
                self._known_buckets.add(bucket_name)
                return PutResult(ok=True)
            except S3Error as exc:
                code, msg, _ = _classify_error(exc)
                if exc.code == "BucketAlreadyOwnedByYou":
                    self._known_buckets.add(bucket_name)
                    return PutResult(ok=True)
                _log.error(
                    f"MinIO 桶检查/创建失败 bucket={bucket_name} code={code} msg={_safe_log_text(msg)}"
                )
                return PutResult(ok=False, error_code=code, error_message=msg, attempts=1)

    def put_file_with_retry(
        self,
        bucket_name: str,
        file: str,
        file_data: str,
        max_retries: int = 3,
        retry_delay_base: float = 1.0,
    ) -> PutResult:
        """写入 MinIO；权限类错误立即失败，网络/503 类错误有限重试。"""
        if max_retries < 1:
            max_retries = 1
        bucket_check = self.ensure_bucket(bucket_name)
        if not bucket_check.ok:
            return bucket_check

        payload = file_data.encode("utf-8")
        data_size = len(payload)
        last_code = ""
        last_msg = ""

        def _put_once() -> None:
            self.client.put_object(
                bucket_name,
                file,
                io.BytesIO(payload),
                data_size,
            )

        for attempt in range(1, max_retries + 1):
            try:
                _put_once()
                return PutResult(ok=True, attempts=attempt)
            except Exception as exc:
                code, msg, retryable = _classify_error(exc)
                last_code, last_msg = code, msg

                if not retryable:
                    _log.error(
                        "MinIO 写入失败(不可重试) bucket=%s file=%s code=%s msg=%s",
                        bucket_name,
                        file,
                        code,
                        _safe_log_text(msg),
                    )
                    return PutResult(
                        ok=False,
                        error_code=code,
                        error_message=msg,
                        attempts=attempt,
                    )

                if attempt >= max_retries:
                    _log.error(
                        "MinIO 写入最终失败(已重试 %s 次) bucket=%s file=%s code=%s msg=%s",
                        max_retries,
                        bucket_name,
                        file,
                        code,
                        _safe_log_text(msg),
                    )
                    return PutResult(
                        ok=False,
                        error_code=code,
                        error_message=msg,
                        attempts=attempt,
                    )

                wait_time = retry_delay_base * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                _log.warning(
                    "MinIO 写入第 %s 次失败, %.1fs 后重试 bucket=%s/%s code=%s msg=%s",
                    attempt,
                    wait_time,
                    bucket_name,
                    file,
                    code,
                    _safe_log_text(msg),
                )
                time.sleep(wait_time)

        return PutResult(
            ok=False,
            error_code=last_code,
            error_message=last_msg,
            attempts=max_retries,
        )


minio_client_61 = ResilientMinIOClient(
    MinIOConfig(
        service=configs_test["url"],
        access_key=configs_test["accessKey"],
        secret_key=configs_test["secretKey"],
    )
)
