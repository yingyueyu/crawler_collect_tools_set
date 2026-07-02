import urllib.parse

import scrapy
from scrapy import cmdline
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from scrapy_redis.spiders import RedisSpider

from tools.key_token_config import PROXY_DEFAULT
from ..utils.create_redis_key import RedisKeyManager
from ..utils.impersonate import (
    apply_impersonate_headers,
    build_impersonate_meta,
    is_valid_go_html,
)
from ..utils.logs import get_default_logger
from ..utils.redis_conf import GetRedis


def normalize_go_task_url(raw: str) -> str:
    """Redis 任务可能是 pkg:golang/...、pkg:go/... 或 pkg.go.dev URL。"""
    from urllib.parse import unquote

    text = (raw or "").strip()
    if not text:
        raise ValueError("empty go task")

    for prefix in ("pkg:golang/", "pkg:go/"):
        if text.startswith(prefix):
            module = unquote(text[len(prefix) :]).strip("/")
            if not module:
                raise ValueError(f"invalid go purl: {text}")
            return f"https://pkg.go.dev/{module}"

    if text.startswith("http://") or text.startswith("https://"):
        parsed = urllib.parse.urlparse(text)
        if "pkg.go.dev" not in parsed.netloc:
            raise ValueError(f"unsupported go url host: {parsed.netloc}")
        module = parsed.path.strip("/").split("?")[0]
        if not module:
            raise ValueError(f"invalid go url: {text}")
        return f"https://pkg.go.dev/{module}"

    if "pkg.go.dev/" in text:
        module = text.split("pkg.go.dev/", 1)[-1].strip("/").split("?")[0]
        if not module:
            raise ValueError(f"invalid go url fragment: {text}")
        return f"https://pkg.go.dev/{module}"

    module = unquote(text).strip("/")
    if not module:
        raise ValueError(f"unsupported go task: {text}")
    return f"https://pkg.go.dev/{module}"


def go_module_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return parsed.path.strip("/").split("?")[0]


class GoHtmlSpider(RedisSpider):
    name = "go_html"
    allowed_domains = ["pkg.go.dev"]
    redis_key = "go_html:urls"
    custom_settings = {
        "CONCURRENT_REQUESTS": 10,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
        "DOWNLOAD_DELAY": 0.5,
        "DOWNLOAD_TIMEOUT": 5,
        "RETRY_TIMES": 1,
        "RETRY_TIMES_403": 1,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "upgrade-insecure-requests": "1",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
        }
        self.http_proxy = PROXY_DEFAULT
        self._redis_client = None
        self.logger = get_default_logger(name="go_html_spider", log_dir="app_logs", max_file_mb=50)

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
            raw = data.decode("utf-8").strip()
            url = normalize_go_task_url(raw)
            headers, impersonate = apply_impersonate_headers(self.headers)
            meta = build_impersonate_meta(
                {"url": url, "proxy": self.http_proxy, 'purl': raw},
                impersonate=impersonate,
            )
            # headers = self.headers
            # meta = {'url': url, 'purl': raw, 'proxy': self.http_proxy}
            yield scrapy.Request(
                url,
                headers=headers,
                callback=self.parse_index,
                errback=self.errback_skip,
                meta=meta,
                dont_filter=True,
            )
        except ValueError as exc:
            self.logger.error(f"跳过无效 Go 任务 {data!r}: {exc}")
            self.redis_client.lpush(self.redis_keys.other_url_key, data.decode("utf-8", errors="replace"))
        except Exception as exc:
            self.logger.error(f"make_request_from_data error: {exc}")

    def parse_index(self, response: HtmlResponse):
        try:
            url = response.meta["url"]
            purl = response.meta["purl"]
            module = go_module_from_url(url)
            file_name = f"{module}/last.html"
            latency = response.meta.get("download_latency", 0)

            if response.status == 200:
                if not is_valid_go_html(response.text):
                    self.logger.warning(f"无效 HTML(疑似挑战页): {url}")
                    self.redis_client.lpush(self.redis_keys.forbidden_key, url)
                    return
                self.logger.info(f"成功 {url} 耗时 {latency:.2f}s")
                yield {
                    "info": "go_html",
                    "url": url,
                    "purl": purl,
                    "name": file_name,
                    "html": response.text,
                    "latency": latency,
                }
            elif response.status == 404:
                yield {
                    "info": "go_html",
                    "url": url,
                    "purl": purl,
                    "name": file_name,
                    "html": "404",
                }
            elif response.status == 500:
                yield {
                    "info": "go_html",
                    "url": url,
                    "purl": purl,
                    "name": file_name,
                    "html": "500",
                }
            elif response.status == 400:
                yield {
                    "info": "go_html",
                    "url": url,
                    "purl": purl,
                    "name": file_name,
                    "html": "400",
                }
            else:
                self.logger.warning(f"状态码 {response.status}: {url}")
        except IgnoreRequest:
            raise
        except Exception as exc:
            self.logger.error(f"parse_index error: {exc}")


if __name__ == "__main__":
    cmdline.execute("scrapy crawl go_html".split())
