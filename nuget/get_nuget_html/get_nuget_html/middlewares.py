# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import json
import time
from datetime import datetime

import redis
import scrapy
from scrapy.core.downloader.handlers.http11 import TunnelError
from twisted.internet.error import (
    ConnectionLost,
    ConnectError,
    TCPTimedOutError,
    ConnectionRefusedError,
    TimeoutError
)
from twisted.web._newclient import ResponseNeverReceived, ResponseFailed

from scrapy.exceptions import IgnoreRequest
from .utils.redis_conf import GetRedis
from .utils.logs import get_default_logger

from tools.key_token_config import PROXY_NUGET
from scrapy.downloadermiddlewares.retry import RetryMiddleware

class GetNugetHtmlDownloaderMiddleware(RetryMiddleware):
    def __init__(self, settings):
        self.redis_client = GetRedis().redis_client(host='127.0.0.1', port=6379, db=1)
        self.time_out_key = 'nuget_html:timeout_urls'
        self.max_retry_times = settings.getint('RETRY_TIMES', 10)
        self.log_tool = get_default_logger(
            name="nuget_html_mid",
            log_dir="app_logs",
            max_file_mb=50,
        )
    def process_response(self, request, response, spider):

        if response.status == 200:
            return response
        if response.status == 429:
            new_request = request.replace(
                meta={
                    **request.meta,
                    'proxy': PROXY_NUGET,
                    'retry_times': request.meta.get('retry_times', 0) + 1
                },
                dont_filter=True
            )
            retries = request.meta.get('retry_times')
            print('retries------------------------', retries)
            if retries is not None:
                if int(retries) >= self.max_retry_times:
                    self.log_tool.info(f"达到最大重试次数:{self.max_retry_times}: {request.url}")
                    self.redis_client.lpush(self.time_out_key, request.url)
                else:
                    return new_request  # 返回新请求触发重试
            else:
                return new_request
        return response
    def process_exception(self, request, exception, spider):
        self.log_tool.debug(f"进入异常处理，异常类型: {type(exception)}")
        try:
            # 捕获所有网络异常类型
            if isinstance(exception, (
                    TCPTimedOutError,
                    ConnectionRefusedError,
                    ConnectionLost,
                    ConnectError,
                    TimeoutError,
                    TunnelError,
                    ResponseNeverReceived,
                    ResponseFailed
            )):
                self.log_tool.warning(f"捕获到网络异常: {type(exception).__name__}, url:{request.url}")

                # --- 代理失效处理 ---


                    # 创建新的Request对象（关键修改）
                new_request = request.replace(
                    meta={
                        **request.meta,
                        'proxy': PROXY_NUGET,
                        'retry_times': request.meta.get('retry_times', 0) + 1
                    },
                    dont_filter=True
                )
                retries = request.meta.get('retry_times')
                print('retries------------------------', retries)
                if retries is not None:
                    if int(retries) >= self.max_retry_times:
                        self.log_tool.info(f"达到最大重试次数:{self.max_retry_times}: {request.url}")
                        self.redis_client.lpush(self.time_out_key, request.url)
                    else:
                        return new_request  # 返回新请求触发重试
                else:
                    return new_request
            else:
                # 无可用代理时记录到Redis
                self.log_tool.error(f'无可用代理，放弃请求: {request.url}')
                raise IgnoreRequest(f"No proxy available for {request.url}")

        except Exception as e:
            self.log_tool.error(f"异常处理出错: {e} {request.url}")
