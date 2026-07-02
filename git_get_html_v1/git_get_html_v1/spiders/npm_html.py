import scrapy
from scrapy import cmdline
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from scrapy_redis.spiders import RedisSpider

from tools.key_token_config import PROXY_DEFAULT
from ..utils.create_redis_key import RedisKeyManager
from ..utils.impersonate import apply_impersonate_headers, build_impersonate_meta, is_valid_npm_html
from ..utils.logs import get_default_logger
from ..utils.redis_conf import GetRedis


class NpmHtmlSpider(RedisSpider):
    name = "npm_html"
    allowed_domains = ["www.npmjs.com"]
    redis_key = "npm_html:urls"
    custom_settings = {
        # 提高吞吐：403 快速放弃，不长时间卡在单条 URL 上
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 1,
        "DOWNLOAD_TIMEOUT": 20,
        "RETRY_TIMES": 1,
        "RETRY_TIMES_403": 1,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "sec-ch-ua-arch": '"x86"',
            "sec-ch-ua-bitness": '"64"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": '""',
            "sec-ch-ua-platform": '"Windows"',
            "sec-ch-ua-platform-version": '"19.0.0"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
        }
        self.http_proxy = PROXY_DEFAULT
        self._redis_client = None
        self.logger = get_default_logger(name="npm_html_spider", log_dir="app_logs", max_file_mb=50)

    @property
    def redis_client(self):
        if self._redis_client is None:
            redis_config = self.settings.get("REDIS_CONFIG")
            self._redis_client = GetRedis().redis_client(
                host=redis_config.get("host"),
                port=redis_config.get("port"),
                db=redis_config.get("db"),
            )
        return self._redis_client

    @property
    def redis_keys(self):
        return RedisKeyManager(self.name)

    def errback_skip(self, failure):
        self.logger.warning(f"请求失败跳过: {failure.request.url} ({failure.value})")


    def make_request_from_data(self, data):
        try:
            url = data.decode("utf-8")
            url = url.lower() if url[-1] != "/" else url[0:-1].lower()
            if url.startswith("pkg:npm/"):
                url = url.replace("pkg:npm/", "https://www.npmjs.com/package/")

            headers, impersonate = apply_impersonate_headers(self.headers)
            meta = build_impersonate_meta(
                {"url": url, "proxy": self.http_proxy},
                impersonate=impersonate,
            )
            yield scrapy.Request(
                url,
                headers=headers,
                callback=self.parse_index,
                errback=self.errback_skip,
                meta=meta,
                dont_filter=True,
            )
        except Exception as e:
            self.logger.error(f"make_request_from_data error: {e}")


    def parse_index(self, response: HtmlResponse):
        try:
            url = response.meta["url"]
            latency = response.meta.get("download_latency", 0)
            module = url.strip("/").split("package/")[-1].replace("%40", "@")
            file = f"{module}/last.html"

            if response.status == 200:
                if not is_valid_npm_html(response.text):
                    self.logger.warning(f"无效 HTML(疑似挑战页): {url}")
                    self.redis_client.lpush(self.redis_keys.forbidden_key, url)
                    return
                self.logger.info(f"成功 {url} 耗时 {latency:.2f}s")
                yield {
                    "info": "npm_html",
                    "url": url,
                    "name": file,
                    "html": response.text,
                    "latency": latency,
                }
            elif response.status == 404:
                yield {
                    "info": "npm_html",
                    "url": url,
                    "name": file,
                    "html": "404",
                }
            elif response.status == 417:
                self.logger.warning(f"417: {url}")
                yield {
                    "info": "npm_html",
                    "url": url,
                    "name": file,
                    "html": "417",
                }
            else:
                self.logger.warning(f"状态码 {response.status}: {url}")
        except IgnoreRequest:
            raise
        except Exception as e:
            self.logger.error(f"parse_index error: {e}")


if __name__ == "__main__":
    cmdline.execute("scrapy crawl npm_html".split())
