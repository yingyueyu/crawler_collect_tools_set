import urllib.parse

import scrapy
from scrapy import cmdline
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from scrapy_redis.spiders import RedisSpider

from tools.key_token_config import PROXY_GITHUB_PYPI
from ..utils.create_redis_key import RedisKeyManager
from ..utils.impersonate import apply_impersonate_headers, build_impersonate_meta
from ..utils.logs import get_default_logger
from ..utils.redis_conf import GetRedis


def normalize_github_task_url(raw: str) -> str:
    """Redis 任务可能是 pkg:github/... 或 github.com URL。"""
    from urllib.parse import unquote

    text = (raw or "").strip()
    if not text:
        raise ValueError("empty github task")

    if text.startswith("pkg:github/"):
        path = unquote(text[len("pkg:github/") :]).strip("/")
        if not path:
            raise ValueError(f"invalid github purl: {text}")
        return f"https://github.com/{path}"

    if text.startswith("http://") or text.startswith("https://"):
        parsed = urllib.parse.urlparse(text)
        if "github.com" not in parsed.netloc:
            raise ValueError(f"unsupported github url host: {parsed.netloc}")
        path = parsed.path.strip("/")
        if not path:
            raise ValueError(f"invalid github url: {text}")
        return f"https://github.com/{path}"

    if "github.com/" in text:
        path = text.split("github.com/", 1)[-1].strip("/").split("?")[0]
        if not path:
            raise ValueError(f"invalid github url fragment: {text}")
        return f"https://github.com/{path}"

    path = unquote(text).strip("/")
    if not path:
        raise ValueError(f"unsupported github task: {text}")
    return f"https://github.com/{path}"


def github_file_from_url(url: str) -> str | None:
    """按原逻辑：1 段 path -> vendor.html，2 段 -> owner/repo/detail.html。"""
    paths = urllib.parse.urlparse(url).path.strip("/").split("/")
    if len(paths) == 1:
        return f"{paths[0]}.html"
    if len(paths) == 2:
        owner, repo = paths[0], paths[1]
        return f"{owner}/{repo}/detail.html"
    return None


class GithubHtmlSpider(RedisSpider):
    name = "github_html"
    allowed_domains = ["github.com"]
    redis_key = "github_html:urls"
    custom_settings = {
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 16,
        "DOWNLOAD_DELAY": 0.3,
        "DOWNLOAD_TIMEOUT": 15,
        "RETRY_TIMES": 1,
        "RETRY_TIMES_403": 1,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "authority": "github.com",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
        }
        self.http_proxy = PROXY_GITHUB_PYPI
        self._redis_client = None
        self.logger = get_default_logger(name="github_html_spider", log_dir="app_logs", max_file_mb=50)

    @property
    def redis_client(self):
        if self._redis_client is None:
            redis_config = self.settings.get("REDIS_CONFIG", {})
            self._redis_client = GetRedis().redis_client(
                host=redis_config.get("host", "127.0.0.1"),
                port=redis_config.get("port", 6379),
                db=redis_config.get("db", 1),
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
            url = normalize_github_task_url(raw)
            url = url.lower() if url[-1] != "/" else url[0:-1].lower()
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
        except ValueError as exc:
            self.logger.error(f"跳过无效 GitHub 任务 {data!r}: {exc}")
            self.redis_client.lpush(
                self.redis_keys.other_url_key,
                data.decode("utf-8", errors="replace"),
            )
        except Exception as exc:
            self.logger.error(f"make_request_from_data error: {exc}")

    def _yield_github_item(self, url, purl, file_name, html, latency=None):
        item = {
            "info": "github_html",
            "url": url,
            "purl": purl,
            "name": file_name,
            "html": html,
        }
        if latency is not None:
            item["latency"] = latency
        return item

    def parse_index(self, response: HtmlResponse):
        try:
            url = response.meta["url"]
            purl = response.meta.get("purl", url)
            file_name = github_file_from_url(url)
            latency = response.meta.get("download_latency", 0)

            if file_name is None:
                self.logger.warning(f"url path length > 2: {url}")
                return

            if response.status == 200:
                self.logger.info(f"成功 {url} 耗时 {latency:.2f}s")
                yield self._yield_github_item(url, purl, file_name, response.text, latency)
            elif response.status == 404:
                yield self._yield_github_item(url, purl, file_name, "404")
            elif response.status == 451:
                yield self._yield_github_item(url, purl, file_name, "disabled")
            else:
                self.logger.warning(f"其他状态码 {response.status}: {url}")
        except IgnoreRequest:
            raise
        except Exception as exc:
            self.logger.error(f"parse_index error: {exc}")


if __name__ == "__main__":
    cmdline.execute("scrapy crawl github_html".split())
