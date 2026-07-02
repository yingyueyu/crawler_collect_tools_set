import io
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import urllib.parse
import urllib3
from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error
from tools.common_logger.logger_common import get_default_logger

from tools.key_token_config import MINIO_168, MINIO_61_TEST, MINIO_99

configs = MINIO_168
configs_99 = MINIO_99
configs_test = MINIO_61_TEST
configs_168 = MINIO_168


@dataclass
class MinIOConfig:
    service: str
    access_key: str
    secret_key: str
    secure: bool = False
    pool_maxsize: int = 16
    timeout: int = 25  # 读超时(秒)


logger = get_default_logger(name='async_minio_client', log_dir='./logs', force_color=True)

# 单对象最大读取 15MB，避免超大 HTML 拖死解析线程
_MAX_OBJECT_BYTES = 15 * 1024 * 1024
# 对象不存在时无需重试的错误码
_NO_RETRY_S3_CODES = frozenset({"NoSuchKey", "NoSuchBucket"})
# 独立 IO 线程池：wait_for 超时后底层线程仍会继续跑，不能用默认小池以免拖死整批
_MINIO_IO_EXECUTOR = ThreadPoolExecutor(max_workers=12, thread_name_prefix="minio-io")
_MINIO_IO_EXECUTOR_SHUTDOWN = False
# 单次阻塞读上限（秒），需小于外层 asyncio.wait_for
_THREAD_READ_TIMEOUT = 27.0
_READ_CHUNK_SIZE = 64 * 1024


def _read_object_with_deadline(response, file: str, deadline: float) -> bytes:
    """分块读取并在总时长超限时主动失败，避免 response.read() 长时间挂死。"""
    import time

    chunks: list[bytes] = []
    total = 0
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"MinIO read deadline exceeded ({_THREAD_READ_TIMEOUT}s): {file}"
            )
        chunk = response.read(_READ_CHUNK_SIZE)
        if not chunk:
            break
        total += len(chunk)
        if total > _MAX_OBJECT_BYTES:
            raise ValueError(
                f"object too large (> {_MAX_OBJECT_BYTES} bytes): {file}"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def shutdown_minio_executor(wait: bool = False) -> None:
    """释放 MinIO IO 线程池；批处理正常结束或 Ctrl+C 时均应调用。"""
    global _MINIO_IO_EXECUTOR_SHUTDOWN
    if _MINIO_IO_EXECUTOR_SHUTDOWN:
        return
    _MINIO_IO_EXECUTOR.shutdown(wait=wait, cancel_futures=True)
    _MINIO_IO_EXECUTOR_SHUTDOWN = True

class AsyncMinIOClient:
    """
    异步 MinIO 客户端包装层。
    底层 minio SDK 为阻塞 I/O，这里通过 asyncio.to_thread 交给线程池执行，
    以便在 asyncio 场景下实现高并发网络读写。
    """

    def __init__(self, config: MinIOConfig):
        self.service = config.service
        self.access_key = config.access_key
        self.secret_key = config.secret_key
        self.secure = config.secure
        
        # 禁用 urllib3 内部重试：否则单次 get_object 可在单线程内阻塞数分钟，
        # 而 asyncio.wait_for 超时后线程仍占用池子，导致后续 URL 永久排队。
        http_client = urllib3.PoolManager(
            maxsize=config.pool_maxsize,
            timeout=urllib3.Timeout(connect=10, read=config.timeout),
            retries=urllib3.Retry(total=0),
        )
        self.client = Minio(
            config.service,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
            http_client=http_client,
        )

    async def get_file(
        self,
        bucket_name: str,
        file: str,
        max_retries: int = 1,
        retry_delay_base: float = 0.5,
        total_timeout: float = 35.0,
    ) -> str | None:
        """
        从 MinIO 获取文件内容。

        - NoSuchKey / NoSuchBucket 立即失败，不重试
        - 整体 asyncio 超时，避免单请求挂死拖住整批
        """
        import asyncio

        bucket_name = str(bucket_name).strip()
        file = str(file).strip()
        if not bucket_name or not file:
            return None

        try:
            return await asyncio.wait_for(
                self._get_file_with_retries(
                    bucket_name,
                    file,
                    max_retries=max_retries,
                    retry_delay_base=retry_delay_base,
                ),
                timeout=total_timeout,
            )
        except asyncio.TimeoutError:
            logger.error(
                f"MinIO获取超时({total_timeout}s): bucket={bucket_name}, file={file}"
            )
            return None

    async def _get_file_with_retries(
        self,
        bucket_name: str,
        file: str,
        *,
        max_retries: int,
        retry_delay_base: float,
    ) -> str | None:
        import asyncio
        import random

        def _inner() -> str:
            import time

            response = self.client.get_object(bucket_name, file)
            try:
                deadline = time.monotonic() + _THREAD_READ_TIMEOUT
                data = _read_object_with_deadline(response, file, deadline)
                return data.decode("utf-8", errors="replace")
            finally:
                response.close()
                response.release_conn()

        loop = asyncio.get_running_loop()
        for attempt in range(1, max_retries + 1):
            try:
                return await asyncio.wait_for(
                    loop.run_in_executor(_MINIO_IO_EXECUTOR, _inner),
                    timeout=_THREAD_READ_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.error(
                    f"MinIO线程读取超时({_THREAD_READ_TIMEOUT}s): "
                    f"bucket={bucket_name}, file={file}, attempt={attempt}/{max_retries}"
                )
                if attempt >= max_retries:
                    return None
                await asyncio.sleep(retry_delay_base)
                continue
            except Exception as e:
                if isinstance(e, (TimeoutError,)):
                    error_type = type(e).__name__
                    error_msg = str(e)
                    if attempt >= max_retries:
                        logger.error(
                            f"MinIO读取超时(已重试{max_retries}次): "
                            f"bucket={bucket_name}, file={file}, msg={error_msg[:150]}"
                        )
                        return None
                    await asyncio.sleep(retry_delay_base)
                    continue
                if isinstance(e, S3Error) and getattr(e, "code", "") in _NO_RETRY_S3_CODES:
                    logger.warning(
                        f"MinIO对象不存在，跳过重试: bucket={bucket_name}, file={file}, code={e.code}"
                    )
                    return None

                error_type = type(e).__name__
                error_msg = str(e)
                is_sdk_bug = "has no attribute" in error_msg

                if attempt >= max_retries:
                    logger.error(
                        f"MinIO获取文件最终失败(已重试{max_retries}次): "
                        f"bucket={bucket_name}, file={file}, "
                        f"type={error_type}, msg={error_msg[:150]}"
                    )
                    return None

                jitter = random.uniform(0, 0.3)
                wait_time = retry_delay_base * (2 ** (attempt - 1)) + jitter
                if is_sdk_bug:
                    wait_time += 1.0

                if attempt > 1:
                    logger.info(
                        f"MinIO第{attempt}次重试{'[SDK Bug]' if is_sdk_bug else ''}: "
                        f"{file} ({error_type})"
                    )
                await asyncio.sleep(wait_time)

        return None

    async def get_bytes(self, bucket_name: str, file: str, max_retries: int = 5, retry_delay_base: float = 2.0) -> bytes | None:
        """
        从MinIO获取文件二进制内容,带指数退避重试机制
        
        Args:
            bucket_name: bucket名称
            file: 文件路径
            max_retries: 最大重试次数(默认5次)
            retry_delay_base: 基础重试延迟秒数(默认2秒,指数退避)
        
        Returns:
            文件内容字节串,失败返回None
        """
        import asyncio

        def _inner() -> bytes | None:
            response = self.client.get_object(bucket_name, file)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()

        # 带指数退避的重试逻辑
        for attempt in range(1, max_retries + 1):
            try:
                return await asyncio.to_thread(_inner)
            except (ConnectionResetError, ConnectionError, OSError, Exception) as e:
                error_type = type(e).__name__
                error_msg = str(e)[:100]
                
                # 如果是最后一次尝试,直接返回None
                if attempt >= max_retries:
                    logger.warning(
                        f"MinIO获取文件失败(已重试{max_retries}次): "
                        f"bucket={bucket_name}, file={file}, "
                        f"error={error_type}: {error_msg}"
                    )
                    return None
                
                # 指数退避: 2s -> 4s -> 8s -> 16s -> 32s
                wait_time = retry_delay_base * (2 ** (attempt - 1))
                
                logger.info(
                    f"MinIO获取文件第{attempt}次失败, {wait_time:.1f}秒后重试: "
                    f"bucket={bucket_name}, file={file}, "
                    f"error={error_type}: {error_msg}"
                )
                
                await asyncio.sleep(wait_time)
        
        return None

    async def put_file(self, bucket_name: str, file: str, file_data: str) -> None:
        import asyncio

        def _inner() -> None:
            payload = file_data.encode("utf-8")
            self.client.put_object(bucket_name, file, io.BytesIO(payload), len(payload))

        await asyncio.to_thread(_inner)

    async def put_bytes(self, bucket_name: str, file: str, bytes_data: bytes) -> None:
        import asyncio

        def _inner() -> None:
            self.client.put_object(bucket_name, file, io.BytesIO(bytes_data), len(bytes_data))

        await asyncio.to_thread(_inner)

    async def remove_file(self, bucket_name: str, file: str) -> None:
        import asyncio

        await asyncio.to_thread(self.client.remove_object, bucket_name, file)

    async def copy_file(self, bucket_name: str, file: str, file_path: str) -> None:
        import asyncio

        await asyncio.to_thread(
            self.client.copy_object, bucket_name, file, CopySource(bucket_name, file_path)
        )


async_minio_client = AsyncMinIOClient(
    MinIOConfig(
        service=configs["url"],
        access_key=configs["accessKey"],
        secret_key=configs["secretKey"],
    )
)

async_minio_client_99 = AsyncMinIOClient(
    MinIOConfig(
        service=configs_99["url"],
        access_key=configs_99["accessKey"],
        secret_key=configs_99["secretKey"],
    )
)

async_minio_client_168 = AsyncMinIOClient(
    MinIOConfig(
        service=configs_168["url"],
        access_key=configs_168["accessKey"],
        secret_key=configs_168["secretKey"],
    )
)

async_minio_client_test = AsyncMinIOClient(
    MinIOConfig(
        service=configs_test["url"],
        access_key=configs_test["accessKey"],
        secret_key=configs_test["secretKey"],
        pool_maxsize=16,
        timeout=25,
    )
)


def minio_client_url_to_path(platform_source: str, bucket_name: str, url: str, html_model: str = 'detail') -> str:
    """
    将minio client url路径根据桶名所对应规则转化为minio本地文件路径
    :param bucket_name: 桶名
    :param url: url路径
    :return: minio本地文件路径
    """
    url = url.strip('\n').strip('/')
    minio_path = None
    if platform_source == 'skills':
        if url.startswith('https://skills.sh/'):
            minio_path = f"{url.replace('https://skills.sh/', '')}/detail.html"
        elif url.startswith('pkg:skills/'):
            minio_path = f"{url.replace('pkg:skills/', '')}/detail.html"
        else:
            minio_path = url
    elif platform_source == 'github':
        if url.startswith('https://github.com/'):
            minio_path = f"{url.replace('https://github.com/', '')}/detail.html"
        elif url.startswith('pkg:github/'):
            minio_path = f"{url.replace('pkg:github/', '')}/detail.html"
        else:
            minio_path = url
    elif platform_source == 'maven':
        if html_model == 'detail':
            groupId, artifactId = urllib.parse.urlparse(url).path.split('/')[-2:]
            minio_path = f"{groupId}/{artifactId}/versions.html"

    elif platform_source == 'protectai':
        if html_model == 'detail':
            # 转化格式
            if url.endswith('detail.html'):
                minio_path = url
            else:
                if url.startswith('https://protectai.com/'):
                    url = url.replace('https://protectai.com', '')
                if not url.startswith('/'):
                    url = f'/{url}'
                if url.endswith('/overview'):
                    url = f'{url}/detail.html'
            minio_path = url

    elif platform_source == 'huggingface':
        if html_model == 'detail':
            if url.endswith('detail.html'):
                minio_path = url
            else:
                if url.startswith('https://huggingface.co/'):
                    url = url.replace('https://huggingface.co/', '')
                if not url.endswith('/main'):
                    url = f'{url}/main'
                if not url.endswith('detail.html'):
                    url = f'{url}/detail.html'
            minio_path = url
    elif platform_source == 'pypi':
        if html_model == 'detail':
            if url.startswith('https://pypi.org/project/'):
                url = url.replace('https://pypi.org/project/', '')
            if not url.endswith('/last.html'):
                url = url.strip('/')
                url = f'{url}/last.html'
            minio_path = url
    elif platform_source == 'npm':
        if html_model == 'detail':
            if url.startswith('https://www.npmjs.com/package/'):
                url = url.replace('https://www.npmjs.com/package/', '')
            if not url.endswith('/last.html'):
                url = f'{url}/last.html'
            minio_path = url
    elif platform_source == 'gitee':
        if html_model == 'detail':
            if url.startswith('https://gitee.com/'):
                url = f"{url.replace('https://gitee.com/', '')}/detail.html"
            elif url.startswith('pkg:gitee/'):
                url = f"{url.replace('pkg:gitee/', '')}/detail.html"
            minio_path = url
    elif platform_source == 'gitlab':
        if html_model == 'detail':
            if url.startswith('https://gitlab.'):
                url = f"{'/'.join(url.strip('/').split('/')[3:])}/detail.html"
            elif url.startswith('pkg:gitlab/'):
                url = f"{url.replace('pkg:gitlab/', '')}/detail.html"
            minio_path = url
    elif platform_source == 'atomgit':
        if html_model == 'detail':
            if url.startswith('https://atomgit.com/'):
                url = f"{url.replace('https://atomgit.com/', '')}/detail.html"
            elif url.startswith('pkg:atomgit/'):
                url = f"{url.replace('pkg:atomgit/', '')}/detail.html"
            minio_path = url
    elif platform_source == 'golang':
        if html_model == 'detail':
            if url.startswith('https://pkg.go.dev/'):
                url = f"{url.replace('https://pkg.go.dev/', '')}/last.html"
            elif url.startswith('pkg:golang/'):
                url = f"{url.replace('pkg:golang/', '')}/last.html"
            minio_path = url

    return minio_path


if __name__ == '__main__':

    import asyncio

    result = asyncio.run(async_minio_client_test.get_file('github-new', 'eunicevassoa/flutter_easy_autocomplete/detail.html'))
    print(result)