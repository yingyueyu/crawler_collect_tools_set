import urllib.parse

import scrapy
from scrapy import cmdline
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from scrapy_redis.spiders import RedisSpider

from tools.key_token_config import PROXY_NUGET
from ..utils.create_redis_key import RedisKeyManager
from ..utils.impersonate import apply_impersonate_headers, build_impersonate_meta
from ..utils.logs import get_default_logger
from ..utils.redis_conf import GetRedis


def normalize_nuget_task_url(raw: str) -> str:
    """Redis 任务可能是 pkg:nuget/...、nuget.org URL 或包 ID。"""
    from urllib.parse import unquote

    text = (raw or "").strip()
    if not text:
        raise ValueError("empty nuget task")

    if text.startswith("pkg:nuget/"):
        package = unquote(text[len("pkg:nuget/") :]).strip("/")
        if not package:
            raise ValueError(f"invalid nuget purl: {text}")
        return f"https://www.nuget.org/packages/{package}"

    if text.startswith("http://") or text.startswith("https://"):
        parsed = urllib.parse.urlparse(text)
        if "nuget.org" not in parsed.netloc:
            raise ValueError(f"unsupported nuget url host: {parsed.netloc}")
        path = parsed.path.strip("/")
        if not path:
            raise ValueError(f"invalid nuget url: {text}")
        return text.rstrip("/")

    if "nuget.org/" in text:
        url = text.split("?")[0].rstrip("/")
        if not urllib.parse.urlparse(url).path.strip("/"):
            raise ValueError(f"invalid nuget url fragment: {text}")
        return url

    package = unquote(text).strip("/")
    if not package:
        raise ValueError(f"unsupported nuget task: {text}")
    return f"https://www.nuget.org/packages/{package}"


def nuget_file_from_url(url: str) -> str:
    owner, repo = urllib.parse.urlparse(url).path.split("/")[-2:]
    return f"{owner}/{repo}/detail.html"


def nuget_purl_from_url(url: str) -> str:
    path = urllib.parse.urlparse(url).path.strip("/")
    if path.startswith("packages/"):
        package = path[len("packages/") :].split("/")[0]
        return f"pkg:nuget/{package}"
    parts = path.split("/")
    if len(parts) >= 2:
        return f"pkg:nuget/{'/'.join(parts[-2:])}"
    return f"pkg:nuget/{parts[-1]}"


class NugetHtmlSpider(RedisSpider):
    name = "nuget_html"
    allowed_domains = ["www.nuget.org"]
    redis_key = "nuget_html:urls"
    custom_settings = {
        "CONCURRENT_REQUESTS": 32,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 32,
        "DOWNLOAD_DELAY": 0,
        "DOWNLOAD_TIMEOUT": 30,
        "RETRY_TIMES": 10,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
        }
        self.http_proxy = PROXY_NUGET
        self._redis_client = None
        self.logger = get_default_logger(name="nuget_html_spider", log_dir="app_logs", max_file_mb=50)

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
            url = normalize_nuget_task_url(raw)
            headers, impersonate = apply_impersonate_headers(self.headers)
            meta = build_impersonate_meta(
                {"url": url, "proxy": self.http_proxy, "purl": raw},
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
            self.redis_client.lpush(self.redis_keys.run_key, url)
        except ValueError as exc:
            self.logger.error(f"跳过无效 NuGet 任务 {data!r}: {exc}")
            self.redis_client.lpush(self.redis_keys.other_url_key, data.decode("utf-8", errors="replace"))
        except Exception as exc:
            self.logger.error(f"make_request_from_data error: {exc}")

    def parse_index(self, response: HtmlResponse):
        try:
            url = response.meta["url"]
            purl = response.meta.get("purl", nuget_purl_from_url(url))
            file_name = nuget_file_from_url(url)
            latency = response.meta.get("download_latency", 0)

            if response.status == 200:
                self.logger.info(f"成功 {url} 耗时 {latency:.2f}s")
                yield {
                    "info": "nuget_html",
                    "url": url,
                    "purl": purl,
                    "name": file_name,
                    "html": response.text,
                    "latency": latency,
                }
            elif response.status == 404:
                yield {
                    "info": "nuget_html",
                    "url": url,
                    "purl": purl,
                    "name": file_name,
                    "html": "404",
                }
            else:
                self.logger.warning(f"状态码 {response.status}: {url}")
        except IgnoreRequest:
            raise
        except Exception as exc:
            self.redis_client.lpush(self.redis_keys.error_key, response.meta["url"])
            self.logger.error(f"parse_index error: {exc} url: {response.meta['url']}")


if __name__ == "__main__":
    cmdline.execute("scrapy crawl nuget_html".split())
