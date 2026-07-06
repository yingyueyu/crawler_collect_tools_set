import json
import urllib
from typing import Iterable

import scrapy
from scrapy import Request

from ..utils.create_redis_key import RedisKeyManager
from ..utils.logs import EnhancedLogger
from ..utils.redis_conf import GetRedis
from scrapy_redis.spiders import RedisSpider
from tools.key_token_config import PROXY_DEFAULT

def normalize_maven_task_url(raw: str) -> str:
    """Redis 任务可能是 pkg:maven/group/artifact 或 mvnrepository URL。"""
    from urllib.parse import unquote

    text = (raw or "").strip()
    if text.startswith("pkg:maven/"):
        rest = unquote(text[len("pkg:maven/") :]).strip("/")
        parts = [p for p in rest.split("/") if p]
        if len(parts) < 2:
            raise ValueError(f"invalid maven purl: {text}")
        url = f"https://mvnrepository.com/artifact/{parts[0]}/{parts[1]}"
        if len(parts) > 2:
            url += "/" + "/".join(parts[2:])
        return url
    if text.startswith("http://") or text.startswith("https://"):
        return text
    if "mvnrepository.com" in text:
        return f"https://{text.lstrip('/')}"
    raise ValueError(f"unsupported maven task: {text}")


# 获取groupId和artifactId
def get_groupId_and_artifactId(purl: str) -> tuple[str, str]:
    if not purl or not isinstance(purl, str):
        raise ValueError(f"invalid maven purl: {purl}")
    if purl.startswith("pkg:maven/"):
        return purl.strip("/").split("/")[1], purl.strip("/").split("/")[2]
    elif purl.startswith("https://mvnrepository.com/artifact/"):
        return purl.strip("/").split("/")[3], purl.strip("/").split("/")[4]
    else:
        raise ValueError(f"invalid maven purl: {purl}")



class MavenHtmlSpider(RedisSpider):
    name = "maven_html"
    allowed_domains = ["mvnrepository.com"]
    redis_key = "maven_html:urls"
    start_urls: list[str] = []
    custom_settings = {
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "USER_AGENT": None,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        # "DOWNLOADER_MIDDLEWARES": {
        #     "scrapy_impersonate.RandomBrowserMiddleware": 1000,
        # },
    }
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5",
            # "Accept-Encoding": "gzip, deflate, br, zstd",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Priority": "u=0, i",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "TE": "trailers"
        }
        self.log_tool = EnhancedLogger.get_logger(
            module_name="maven_html_spider",
            log_dir="app_logs",
            rotation="50 MB"
        )
        self.redis_key_manager = RedisKeyManager(self.name)
        self.http_proxy = PROXY_DEFAULT
        self._redis_client = None

    @property
    def redis_client(self):
        # 延迟初始化 Redis 客户端
        if self._redis_client is None:
            redis_config = self.settings.get('REDIS_CONFIG')
            self._redis_client = GetRedis().redis_client(
                host=redis_config.get('host'),
                port=redis_config.get('port'),
                db=redis_config.get('db')
            )
        return self._redis_client

    # def start_requests(self) -> Iterable[Request]:
    #     url = 'https://mvnrepository.com/artifact/io.streamnative.pulsar.handlers/tests-common'
    #     paths = urllib.parse.urlparse(url).path.strip('/').split('/')
    #     if len(paths) <= 3:
    #
    #         yield Request(url, callback=self.parse_, headers=self.headers, dont_filter=True, meta={'proxy': self.http_proxy, 'url':  url})
    #         self.redis_client.lpush(self.redis_key_manager.run_key,  url)
    #     elif len(paths) > 3:
    #
    #         yield Request(url, callback=self.parse_first_version, headers=self.headers, dont_filter=True, meta={'proxy': self.http_proxy, 'url':  url})
    #         self.redis_client.lpush(self.redis_key_manager.run_key,  url)
    #
    #     else:
    #         self.redis_client.lpush(self.redis_key_manager.other_url_key, url)

    def make_request_from_data(self, data):
        raw = data.decode("utf-8").strip()
        try:
            groupId, artifactId = get_groupId_and_artifactId(raw)
            url = normalize_maven_task_url(raw)
            purl = f"pkg:maven/{groupId}/{artifactId}"
        except ValueError as exc:
            self.log_tool.error(f"跳过无效任务 {raw!r}: {exc}")
            self.redis_client.lpush(self.redis_key_manager.other_url_key, raw)
            return

        paths = urllib.parse.urlparse(url).path.strip("/").split("/")
        if len(paths) <= 3:

            yield Request(url, callback=self.parse_, headers=self.headers, dont_filter=True,
                          meta={'proxy': self.http_proxy, 'url': url, 'purl': purl})
            self.redis_client.lpush(self.redis_key_manager.run_key, url)
        # elif len(paths) > 3:

        #     yield Request(url, callback=self.parse_first_version, headers=self.headers, dont_filter=True,
        #                   meta={'proxy': self.http_proxy, 'url': url})
        #     self.redis_client.lpush(self.redis_key_manager.run_key, url)

        else:
            self.redis_client.lpush(self.redis_key_manager.other_url_key, url)
    def parse_(self, response):
        url = response.meta['url']
        if response.xpath('//li[@class="active"]/a[1]'):
            a_str = ','.join(response.xpath('//div[@id="snippets"]//li/a/text()').getall())
            print(a_str)
            if 'Central' in a_str or 'Google' in a_str:
                yield from self.parse_index(response)
            else:
                self.log_tool.debug(f"不是中央仓库或谷歌仓")
                self.redis_client.lpush(self.redis_key_manager.not_Central_Google_key, url)

    def parse_index(self, response):
        url = response.meta['url']
        groupId, artifactId = get_groupId_and_artifactId(url)
        filename = f'{groupId}/{artifactId}/versions.html'
        if response.status == 200:
            # if response.xpath("//a[contains(@class, 'vbtn')][1]/@href"):
                # first_version_url = url.replace(url.split('/')[-1], '') + response.xpath("//a[contains(@class, 'vbtn')][1]/@href").get()
            try:
                
                print(response.status)
                data = {
                    'data': 'index_html',
                    'filename': filename,
                    'html': response.text,
                    'url': url,
                    'purl': response.meta['purl'],
                }
                if response.meta.get('new_index_url'):
                    data['url'] = response.meta['new_index_url']
                    yield data
                    self.redis_client.lpush(self.redis_key_manager.old_new_url_key,
                                            json.dumps({'old_url': response.meta['old_url'], 'new_url': response.meta['new_index_url']}))
                    self.redis_client.lrem(self.redis_key_manager.run_key, 0, response.meta['old_url'])
                else:
                    yield data
            except Exception as e:
                self.log_tool.error(f"处理组件页面出错: {url}, 错误: {e}")
                # yield Request(first_version_url, callback=self.parse_first_version, headers=self.headers, dont_filter=True, meta={'proxy': self.http_proxy, 'url':  first_version_url})

                # self.redis_client.lpush(self.redis_key_manager.run_key,  url)
            # else:
            #     self.log_tool.debug(f"没有找到版本，尝试获取上层组件页面")
            #     if response.xpath('//div[@class="breadcrumb"]/a[3]/@href'):
            #         new_index_url = url.replace(response.url.split('/')[-1], '') + response.xpath('//div[@class="breadcrumb"]/a[3]/@href').get()
            #         yield Request(new_index_url, callback=self.parse_index, headers=self.headers, dont_filter=True, meta={'proxy': self.http_proxy, 'new_index_url':  new_index_url, 'old_url':  url, 'url':  url})
            #         self.redis_client.lpush(self.redis_key_manager.run_key, new_index_url)
            #     else:
            #         self.redis_client.lpush(self.redis_key_manager.not_version_key,  url)
            #         self.redis_client.lrem(self.redis_key_manager.run_key, 0, url)
        elif response.status == 404:
            self.log_tool.error(f"404 页面: {url}")
            self.redis_client.lpush(self.redis_key_manager.req_404_key,  url)
            self.redis_client.lrem(self.redis_key_manager.run_key, 0, url)
            yield {
                'data': 'index_html_404',
                'filename': filename,
                'html': '404',
                'url': url,
                'purl': response.meta['purl'],
            }
    # def parse_first_version(self, response):
    #     url = response.meta['url']
    #     if response.status == 200:
    #         try:
    #             groupId, artifactId, version = urllib.parse.urlparse(response.url).path.split('/')[-3:]
    #             filename = f'{groupId}/{artifactId}/first_version.html'
    #             yield {
    #                 'data': 'first_version_html',
    #                 'filename': filename,
    #                 'html': response.text,
    #                 'url': url,
    #             }
    #         except Exception as e:
    #             self.log_tool.error(f"处理版本页面出错: {url}, 错误: {e}")


