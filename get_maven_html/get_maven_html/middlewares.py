# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from scrapy.core.downloader.handlers.http11 import TunnelError
from twisted.internet.error import (
    ConnectionLost,
    ConnectError,
    TCPTimedOutError,
    ConnectionRefusedError,
    TimeoutError
)
from twisted.web._newclient import ResponseNeverReceived, ResponseFailed
from .utils.logs import EnhancedLogger
from .utils.redis_conf import GetRedis
from .utils.create_redis_key import RedisKeyManager
from scrapy.exceptions import IgnoreRequest
from scrapy_impersonate.middleware import RandomBrowserMiddleware
# 添加对scrapy_impersonate相关异常的导入
try:
    from curl_cffi.requests.exceptions import ProxyError as CurlCffiProxyError
    from curl_cffi.requests.exceptions import RequestsError as CurlCffiRequestsError
except ImportError:
    try:
        from curl_cffi.requests.errors import ProxyError as CurlCffiProxyError
        from curl_cffi.requests.errors import RequestsError as CurlCffiRequestsError
    except ImportError:
        CurlCffiProxyError = type('CurlCffiProxyError', (Exception,), {})
        CurlCffiRequestsError = type('CurlCffiRequestsError', (Exception,), {})

# 尝试导入CurlError
try:
    from curl_cffi.curl import CurlError as CurlCffiCurlError
except ImportError:
    try:
        from curl_cffi.requests.errors import CurlError as CurlCffiCurlError
    except ImportError:
        CurlCffiCurlError = type('CurlCffiCurlError', (Exception,), {})


class GetMavenHtmlDownloaderMiddleware(RandomBrowserMiddleware):
    def __init__(self, settings):
        super().__init__(settings)
        # self.redis_client = GetRedis().redis_client(host='47.239.232.1', port=16379, db=0)
        redis_config = settings.get('REDIS_CONFIG')
        self.redis_client = GetRedis().redis_client(
            host=redis_config.get('host'),
            port=redis_config.get('port'),
            db=redis_config.get('db')
        )
        self.log_tool = EnhancedLogger.get_logger(
            module_name="WSO2_Public_api_mid",
            log_dir="app_logs",
            rotation="50 MB"
        )
        self.http_proxy = "http://u1592102137128011:YBTf2immQggj@proxy.123proxy.cn:35923"

        self.max_retry_times = settings.getint('RETRY_TIMES', 10)
        # self.timeout_urls_key =
        # self.run_key = 'WSO2_Public_api:run_urls'
        # self.error_key = 'WSO2_Public_api:error_urls'
        # self.other_code_urls_key = 'WSO2_Public_api:other_code_urls'
        # self.req_404_key = 'WSO2_Public_api:req_404_urls'

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler.settings)
        obj.keys = RedisKeyManager(crawler.spider.name)
        return obj


    def process_response(self, request, response, spider):
        try:
            if response.status == 200:
                return response
            elif response.status in [404]:
                spider.logger.debug(f"忽略状态码 {response.status} 的响应: {response.url}")
                self.redis_client.lpush(self.keys.req_404_key, request.url)
                # spider.logger.debug(f"忽略状态码111111111111111111 {response.status} 的响应: {response.url}")
                return response
            elif response.status in [429, 403]:
                request_ = request.copy()
                request_.meta['proxy'] = self.http_proxy
                spider.logger.debug(f"请求 {response.status} 更换代理: {self.http_proxy}")
                return self._retry_or_fail(request_, spider)
            else:
                # spider.logger.warning(f"异常状态码 {response.status}: {response.url}")
                # # time.sleep(1222)
                # self.redis_client.lpush(self.other_code_urls_key,
                #                         json.dumps({'url': request.url, 'status': response.status}))
                return response
        except IgnoreRequest:
            raise
        except Exception as e:
            self.log_tool.error(f'process_response发生错误！！  {e}！')
            return response

    def process_exception(self, request, exception, spider):
        try:
            if isinstance(exception, (
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
                    CurlCffiCurlError
            )) or "CONNECT tunnel failed" in str(exception) or "Failed to perform, curl:" in str(exception):
                
                if type(exception).__name__  == 'SSLError' or 'SSLError' in str(exception):
                    spider.logger.warn(f"捕获到SSL异常: {type(exception).__name__}, url:{request.url}\n{str(exception)}")
                    # 直接从redis中删除这个url，放回到待爬队列
                    self.redis_client.lrem(self.keys.run_key, 0, request.url)
                    self.redis_client.lpush(self.keys.urls_key, request.url)
                    raise IgnoreRequest()
                else:
                    spider.logger.warn(f"捕获到网络异常: {type(exception).__name__}, url:{request.url}\n{str(exception)}")
                    return self._retry_or_fail(request, spider)
            else:
                self.log_tool.error(f"其他异常: {type(exception).__name__} - {request.url}")
                self.redis_client.lrem(self.keys.run_key, 0, request.url)
                self.redis_client.lpush(self.keys.error_key, request.url)
        except IgnoreRequest:
            raise
        except Exception as e:
            self.log_tool.error(f"处理异常时发生错误: {e}")

    def _retry_or_fail(self, request, spider):
        retries = request.meta.get('retry_times', 0) + 1
        if retries <= self.max_retry_times:
            spider.logger.info(f"重试 {retries}/{self.max_retry_times}: {request.url}")
            retryreq = request.copy()
            retryreq.meta['retry_times'] = retries
            retryreq.dont_filter = True
            return retryreq
        self.log_tool.error(f"超过最大重试次数，放弃请求: {request.url}")
        self.redis_client.lpush(self.keys.timeout_key, request.url)
        self.redis_client.lrem(self.keys.run_key, 0, request.url)
        raise IgnoreRequest()