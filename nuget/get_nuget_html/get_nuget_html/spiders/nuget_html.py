import json
import urllib.parse

import scrapy
from scrapy.http import HtmlResponse
from scrapy_redis.spiders import RedisSpider
from scrapy import cmdline

from tools.key_token_config import PROXY_NUGET
from ..utils.redis_conf import GetRedis
from ..utils.logs import get_default_logger

class NugetHtmlSpider(RedisSpider):
    name = "nuget_html"
    allowed_domains = ["www.nuget.org"]
    # start_urls = ["https://www.nuget.org/"]
    redis_key = 'nuget_html:urls'
    def __init__(self):
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "sec-ch-ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        self.redis_client = GetRedis().redis_client(host='127.0.0.1', port=6379, db=1)
        self.run_key = 'nuget_html:run_urls'
        self.other_code_urls_key = 'nuget_html:other_code_urls'
        self.error_urls_key = 'nuget_html:error_urls'
        self.req_404_key = 'nuget_html:req_404_urls'
        self.log_tool = get_default_logger(
            name="nuget_html_spider",
            log_dir="app_logs",
            max_file_mb=50,
        )

    def make_request_from_data(self, data):
        url = ''
        try:
            url = data.decode()
            https_proxy = PROXY_NUGET

            yield scrapy.Request(url, headers=self.headers, callback=self.parse, meta={'url': url, 'proxy': https_proxy})
            self.redis_client.lpush(self.run_key, url)
        except Exception as e:
            self.log_tool.error(f'redis数据读取错误！！！{e}, url：{url}')

    def parse(self, response):
        try:
            owner, repo = urllib.parse.urlparse(response.meta['url']).path.split('/')[-2:]
            file = f'{owner}/{repo}/detail.html'
            if response.status == 200:
                print(response.status)

                yield {
                    'info': 'nuget_html',
                    'url': response.meta['url'],
                    'name': file,
                    'html': response.text,
                }
            elif response.status == 404:
                self.redis_client.lpush(self.req_404_key, response.meta['url'])
                yield {
                    'info': 'nuget_html_404',
                    'url': response.meta['url'],
                    'name': file,
                    'html': '404',
                }
            else:
                self.redis_client.lpush(self.other_code_urls_key, json.dumps({'url': response.meta['url'], 'status': response.status}))
        except Exception as e:
            self.redis_client.lpush(self.error_urls_key, response.meta['url'])
            self.log_tool.error(f'解析nuget出错！！！ 错误信息：{e} url：{response.meta["url"]}')
