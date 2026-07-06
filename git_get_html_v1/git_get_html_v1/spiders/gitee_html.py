import json
import urllib.parse

import scrapy
from scrapy import cmdline
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from scrapy_redis.spiders import RedisSpider

from tools.key_token_config import PROXY_GITHUB_PYPI
from ..utils.create_redis_key import RedisKeyManager
from ..utils.logs import get_default_logger
from ..utils.redis_conf import GetRedis

# Gitee BDWAF 会拦截 Mozilla/* 浏览器 UA；使用非浏览器 UA 可绕过 405 挑战页。
GITEE_USER_AGENT = "git-get-html-bot/1.0"

GITEE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "User-Agent": GITEE_USER_AGENT,
}

GITEE_WAF_MARKERS = (
    "__noxExpire",
    "__noxImd",
    "nox_20260413.js",
    "gangplank_",
)


def normalize_gitee_task_url(raw: str) -> str:
    """Redis 任务可能是 pkg:gitee/owner/repo 或 gitee.com URL。"""
    from urllib.parse import unquote

    text = (raw or "").strip()
    if not text:
        raise ValueError("empty gitee task")

    if text.startswith("pkg:gitee/"):
        path = unquote(text[len("pkg:gitee/") :]).strip("/")
        if not path:
            raise ValueError(f"invalid gitee purl: {text}")
        return f"https://gitee.com/{path}"

    if text.startswith("http://") or text.startswith("https://"):
        parsed = urllib.parse.urlparse(text)
        if "gitee.com" not in parsed.netloc:
            raise ValueError(f"unsupported gitee url host: {parsed.netloc}")
        path = parsed.path.strip("/")
        if not path:
            raise ValueError(f"invalid gitee url: {text}")
        return f"https://gitee.com/{path}"

    if "gitee.com/" in text:
        path = text.split("gitee.com/", 1)[-1].strip("/").split("?")[0]
        if not path:
            raise ValueError(f"invalid gitee url fragment: {text}")
        return f"https://gitee.com/{path}"

    path = unquote(text).strip("/")
    if not path:
        raise ValueError(f"unsupported gitee task: {text}")
    return f"https://gitee.com/{path}"


def gitee_file_from_url(url: str) -> str | None:
    """2 段 path -> owner/repo/detail.html。"""
    paths = urllib.parse.urlparse(url).path.strip("/").split("/")
    if len(paths) == 2:
        owner, repo = paths[0], paths[1]
        return f"{owner}/{repo}/detail.html"
    return None


def is_gitee_waf_shell(text: str) -> bool:
    if not text:
        return True
    lower = text.lower()
    return any(marker.lower() in lower for marker in GITEE_WAF_MARKERS)


def is_valid_gitee_html(text: str) -> bool:
    """过滤 BDWAF 405 挑战页及 Markdown 摘要等非目标 HTML。"""
    if not text or len(text) < 500 or is_gitee_waf_shell(text):
        return False
    lower = text.lower()
    if "gitee.com" not in lower:
        return False
    return (
        "git-project" in lower
        or "project-right-sidebar" in lower
        or "social-stat-item" in lower
        or ("<!doctype html" in lower and "lang=" in lower)
    )


class GiteeHtmlSpider(RedisSpider):
    name = "gitee_html"
    allowed_domains = ["gitee.com"]
    redis_key = "gitee_html:urls"
    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 0.5,
        "DOWNLOAD_TIMEOUT": 15,
        "RETRY_TIMES": 1,
        "RETRY_TIMES_403": 1,
        # 仅 gitee spider 生效，不影响其他源
        "HTTPERROR_ALLOWED_CODES": [
            404, 403, 405, 429, 500, 502, 503, 504, 408, 400, 202, 451, 417,
        ],
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_proxy = PROXY_GITHUB_PYPI
        self._redis_client = None
        self.log_tool = get_default_logger(name="gitee_html_spider", log_dir="app_logs", max_file_mb=50)

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
        self.log_tool.warning(f"请求失败跳过: {failure.request.url} ({failure.value})")

    def make_request_from_data(self, data):
        try:
            raw = data.decode("utf-8").strip()
            url = normalize_gitee_task_url(raw)
            url = url.lower() if url[-1] != "/" else url[0:-1].lower()
            meta = {
                "url": url,
                "purl": raw,
                "proxy": self.http_proxy,
            }
            yield scrapy.Request(
                url,
                headers=dict(GITEE_HEADERS),
                callback=self.parse_index,
                errback=self.errback_skip,
                meta=meta,
                dont_filter=True,
            )
        except ValueError as exc:
            self.log_tool.error(f"跳过无效 Gitee 任务 {data!r}: {exc}")
            self.redis_client.lpush(
                self.redis_keys.other_url_key,
                data.decode("utf-8", errors="replace"),
            )
        except Exception as exc:
            self.log_tool.error(f"make_request_from_data error: {exc}")

    def _yield_gitee_item(self, url, purl, file_name, html, latency=None):
        item = {
            "info": "gitee_html",
            "url": url,
            "purl": purl,
            "name": file_name,
            "html": html,
        }
        if latency is not None:
            item["latency"] = latency
        return item

    def _record_waf_blocked(self, url, status):
        self.log_tool.warning(f"Gitee WAF/无效响应 status={status}: {url}")
        self.redis_client.lpush(
            self.redis_keys.other_code_key,
            json.dumps({"url": url, "status": status}),
        )

    def parse_index(self, response: HtmlResponse):
        try:
            url = response.meta["url"]
            purl = response.meta.get("purl", url)
            file_name = gitee_file_from_url(url)
            latency = response.meta.get("download_latency", 0)

            if file_name is None:
                self.log_tool.warning(f"url path length != 2: {url}")
                return

            if response.status == 200:
                if not is_valid_gitee_html(response.text):
                    self._record_waf_blocked(url, response.status)
                    return
                self.log_tool.info(f"成功 {url} 耗时 {latency:.2f}s")
                yield self._yield_gitee_item(url, purl, file_name, response.text, latency)
            elif response.status == 404:
                yield self._yield_gitee_item(url, purl, file_name, "404")
            elif response.status == 405:
                self._record_waf_blocked(url, response.status)
            else:
                self.log_tool.warning(f"其他状态码 {response.status}: {url}")
        except IgnoreRequest:
            raise
        except Exception as exc:
            self.log_tool.error(f"parse_index error: {exc}")


if __name__ == "__main__":
    cmdline.execute("scrapy crawl gitee_html".split())
