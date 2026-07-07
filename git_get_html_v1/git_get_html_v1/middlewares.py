# -*- coding: utf-8 -*-
import json
import re

import scrapy
from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from twisted.internet import threads
from twisted.internet.error import (
    ConnectionLost,
    ConnectError,
    TCPTimedOutError,
    ConnectionRefusedError,
    TimeoutError,
)
from twisted.web._newclient import ResponseNeverReceived, ResponseFailed

from .utils.logs import get_default_logger
from .utils.impersonate import build_impersonate_meta, pick_impersonate, sync_request_impersonate
from .utils.minute_stats import inc_403
from .utils.redis_conf import GetRedis

from .utils.create_redis_key import RedisKeyManager

from tools.key_token_config import PROXY_DEFAULT

try:
    from curl_cffi.requests.exceptions import ProxyError as CurlCffiProxyError
    from curl_cffi.requests.exceptions import RequestsError as CurlCffiRequestsError
except ImportError:
    try:
        from curl_cffi.requests.errors import ProxyError as CurlCffiProxyError
        from curl_cffi.requests.errors import RequestsError as CurlCffiRequestsError
    except ImportError:
        CurlCffiProxyError = type("CurlCffiProxyError", (Exception,), {})
        CurlCffiRequestsError = type("CurlCffiRequestsError", (Exception,), {})

try:
    from curl_cffi.curl import CurlError as CurlCffiCurlError
except ImportError:
    try:
        from curl_cffi.requests.errors import CurlError as CurlCffiCurlError
    except ImportError:
        CurlCffiCurlError = type("CurlCffiCurlError", (Exception,), {})

PYPI_CLOUDFLARE_MARKERS = (
    "A required part of this site couldn't load. This may be due to a browser",
    "A required part of this site couldn\u2019t load. This may be due to a browse",
)

_SSL_CONNECTION_ERROR_NAMES = frozenset(
    {
        "SSLError",
        "SSLCertVerificationError",
        "SSLZeroReturnError",
        "ConnectionError",
        "ConnectionResetError",
        "ConnectionAbortedError",
        "BrokenPipeError",
    }
)


class GitGetHtmlDownloaderMiddleware:
    """curl_cffi 下载中间件：不继承 RandomBrowserMiddleware，避免随机覆盖 impersonate。"""

    def __init__(self, settings):
        redis_config = settings.get("REDIS_CONFIG")
        self.redis_client = GetRedis().redis_client(
            host=redis_config.get("host"),
            port=redis_config.get("port"),
            db=redis_config.get("db"),
        )
        self.log_tool = get_default_logger(
            name="git_get_html_v1_mid",
            log_dir="app_logs",
            max_file_mb=50,
        )
        self.http_proxy = PROXY_DEFAULT
        self.max_retry_times = settings.getint("RETRY_TIMES", 10)

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler.settings)
        obj.keys = RedisKeyManager(crawler.spider.name)
        return obj

    def process_request(self, request, spider):
        # spider 已设置 impersonate 时保持不动，仅同步 headers
        if request.meta.get("impersonate"):
            sync_request_impersonate(request)
        # PyPI 过盾必须在同一 curl_cffi Session 内完成，不能拆成多个 Scrapy 请求
        if spider.name == "pypi_html" and request.meta.get("pypi_cf_handled"):
            d = threads.deferToThread(self._fetch_pypi_html, request)
            d.addCallback(self._build_pypi_html_response, request)
            return d
        return None

    @staticmethod
    def _fetch_pypi_html(request):
        from .utils.pypi_cf_fetch import fetch_pypi_html

        return fetch_pypi_html(request.meta["url"], proxy=request.meta.get("proxy"))

    @staticmethod
    def _build_pypi_html_response(result, request):
        html_text, status, latency = result
        response = HtmlResponse(
            url=request.meta["url"],
            status=status or 500,
            body=(html_text or "").encode("utf-8"),
            encoding="utf-8",
            request=request,
        )
        response.meta["download_latency"] = latency
        return response

    def _is_pypi_cloudflare(self, response):
        return any(marker in response.text for marker in PYPI_CLOUDFLARE_MARKERS)

    def _build_pypi_reload_request(self, request, response):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "max-age=0",
            "Upgrade-Insecure-Requests": "1",
        }
        reload_url = "https://pypi.org/_fs-ch-1T1wmsGaOgGaSxcX/script.js?reload=true"
        set_cookie_header = response.headers.get(b"Set-Cookie")
        if not set_cookie_header:
            return None

        set_cookie = re.findall("b'(.*?);", str(set_cookie_header))
        if not set_cookie:
            return None

        headers["cookie"] = set_cookie[0]
        headers["referer"] = request.meta.get("url", request.url)
        proxy = request.meta.get("proxy", self.http_proxy)
        meta = build_impersonate_meta(
            {
                "set_cookie": set_cookie[0],
                "url": request.meta.get("url", request.url),
                "proxy": proxy,
            },
            impersonate=request.meta.get("impersonate"),
        )
        return scrapy.Request(
            url=reload_url,
            headers=headers,
            dont_filter=True,
            meta=meta,
        )

    def process_response(self, request, response, spider):
        try:
            if response.status != 200:
                ua = response.request.headers.get(b"User-Agent", b"").decode()
                self.log_tool.warning(
                    f"impersonate={request.meta.get('impersonate')}, "
                    f"status={response.status}, url={request.url}, ua={ua[:60]}"
                )

            if response.status == 407:
                self.log_tool.error(f"代理认证失败(407): {request.url}")
                self.redis_client.lpush(self.keys.timeout_key, request.url)
                raise IgnoreRequest()

            # pypi_html 爬虫在 spider 内显式走 script.js → PoW → 再 GET 全流程
            if spider.name != "pypi_html" and self._is_pypi_cloudflare(response):
                reload_req = self._build_pypi_reload_request(request, response)
                if reload_req is not None:
                    return reload_req

            if response.status == 400:
                return response
            if response.status == 200:
                return response
            if response.status == 404:
                self.redis_client.lpush(self.keys.req_404_key, request.url)
                return response
            if response.status == 403:
                return self._retry_403_or_skip(request, spider)
            if response.status == 429:
                return self._retry_or_fail(request, spider, rotate_fingerprint=True)
            if response.status == 417:
                return response

            self.redis_client.lpush(
                self.keys.other_code_key,
                json.dumps({"url": request.url, "status": response.status}),
            )
            return response
        except IgnoreRequest:
            raise
        except Exception as e:
            self.log_tool.error(f"process_response发生错误: {e}")
            return response

    def process_exception(self, request, exception, spider):
        try:
            error_type = exception.__class__.__name__
            self.log_tool.error(f"请求异常 {error_type}: {request.url}")
            self.log_tool.error(f"请求异常 {request.url}: {exception}")



            if isinstance(exception, TunnelError):
                error_str = str(exception)
                if "407" in error_str or "Proxy Authentication Required" in error_str:
                    self.log_tool.error(f"代理认证失败(TunnelError 407): {request.url}")
                    self.redis_client.lpush(self.keys.timeout_key, request.url)
                    raise IgnoreRequest()

            err_msg = str(exception)
            if "407" in err_msg or "proxy authentication" in err_msg.lower():
                self.log_tool.error(f"代理认证失败(407): {request.url}")
                self.redis_client.lpush(
                    self.keys.timeout_key,
                    request.meta.get("url", request.url),
                )
                raise IgnoreRequest()

            if isinstance(
                exception,
                (
                    TCPTimedOutError,
                    ConnectionRefusedError,
                    ConnectionLost,
                    ConnectError,
                    TimeoutError,
                    TunnelError,
                    ResponseNeverReceived,
                    ResponseFailed,
                    CurlCffiProxyError,
                    CurlCffiRequestsError,
                    CurlCffiCurlError,
                ),
            ) or "CONNECT tunnel failed" in str(exception) or "Failed to perform, curl:" in str(exception):
                rotate = self._is_ssl_or_connection_error(exception)
                return self._retry_or_fail(
                    request,
                    spider,
                    rotate_fingerprint=rotate,
                    requeue_on_fail=rotate,
                )

            self.log_tool.error(f"其他异常: {type(exception).__name__} - {request.url}")
            self.redis_client.lpush(self.keys.error_key, request.url)
        except IgnoreRequest:
            raise
        except Exception as e:
            self.log_tool.error(f"处理异常时发生错误: {e}")

    def _get_pending_queue_key(self, spider):
        return getattr(spider, "redis_key", f"{spider.name}:urls")

    def _task_key(self, request):
        """scrapy-redis 待爬队列里的原始任务（优先 purl）。"""
        return request.meta.get("purl") or request.meta.get("url", request.url)

    def _is_ssl_or_connection_error(self, exception):
        if exception.__class__.__name__ in _SSL_CONNECTION_ERROR_NAMES:
            return True
        if isinstance(
            exception,
            (
                ConnectionLost,
                ConnectError,
                ConnectionRefusedError,
                ResponseNeverReceived,
                ResponseFailed,
                CurlCffiProxyError,
            ),
        ):
            return True
        msg = str(exception).lower()
        return (
            "ssl" in msg
            or "connection reset" in msg
            or "connection was reset" in msg
            or "connect tunnel failed" in msg
            or "wrong version number" in msg
            or "connection aborted" in msg
        )

    def _requeue_to_pending(self, request, spider, reason=""):
        """放弃当前请求时，将任务放回 scrapy-redis 待爬队列。"""
        task = self._task_key(request)
        url = request.meta.get("url", request.url)
        # 待爬队列需要 URL；purl 由 spider 的 normalize_gitlab_task 等再解析
        requeue_task = url if str(task).startswith("pkg:") else task
        queue_key = self._get_pending_queue_key(spider)
        self.redis_client.lrem(self.keys.run_key, 0, task)
        if url != task:
            self.redis_client.lrem(self.keys.run_key, 0, url)
        self.redis_client.lpush(queue_key, requeue_task)
        msg = f"放回待爬队列 {queue_key}: {requeue_task}"
        if reason:
            msg = f"{reason}，{msg}"
        self.log_tool.warning(msg)

    def _get_403_retry_limit(self, spider):
        return spider.settings.getint("RETRY_TIMES_403", 1)

    def _retry_403_or_skip(self, request, spider):
        retries = request.meta.get("403_retry_times", 0) + 1
        limit = self._get_403_retry_limit(spider)
        if retries <= limit:
            retryreq = request.copy()
            retryreq.meta["403_retry_times"] = retries
            retryreq.dont_filter = True
            sync_request_impersonate(retryreq, impersonate=pick_impersonate())
            self.log_tool.info(f"403 重试 {retries}/{limit}: {request.url}")
            return retryreq

        self._requeue_to_pending(request, spider, reason="403 重试已达上限")
        inc_403()
        raise IgnoreRequest()

    def _retry_or_fail(
        self,
        request,
        spider,
        rotate_fingerprint=False,
        requeue_on_fail=False,
    ):
        try:
            retries = request.meta.get("retry_times", 0) + 1
            if retries <= self.max_retry_times:
                current_timeout = request.meta.get("download_timeout", 30)
                new_timeout = min(current_timeout + 10, 60)
                fingerprint_note = ""
                if rotate_fingerprint:
                    fingerprint_note = "，换指纹"
                self.log_tool.info(
                    f"重试 {retries}/{self.max_retry_times} "
                    f"(超时:{new_timeout}s{fingerprint_note}): {request.url}"
                )
                retryreq = request.copy()
                retryreq.meta["retry_times"] = retries
                retryreq.meta["download_timeout"] = new_timeout
                retryreq.meta["proxy"] = request.meta.get("proxy") or self.http_proxy
                retryreq.dont_filter = True
                if rotate_fingerprint:
                    new_fp = sync_request_impersonate(retryreq, impersonate=pick_impersonate())
                    self.log_tool.info(f"重试指纹 -> {new_fp}: {request.url}")
                return retryreq

            self.log_tool.error(f"超过最大重试次数，放弃请求: {request.url}")
            if requeue_on_fail:
                self._requeue_to_pending(
                    request,
                    spider,
                    reason="SSL/Connection 重试已达上限",
                )
            else:
                self.redis_client.lpush(self.keys.timeout_key, request.url)
            raise IgnoreRequest()
        except IgnoreRequest:
            raise
        except Exception as e:
            self.log_tool.error(f"重试异常: {e}")
            raise IgnoreRequest()
