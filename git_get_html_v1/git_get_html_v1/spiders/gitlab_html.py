import json
import urllib.parse

import scrapy
from scrapy import cmdline
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from scrapy_redis.spiders import RedisSpider
from lxml import html

from tools.key_token_config import PROXY_GITHUB_PYPI
from ..utils.create_redis_key import RedisKeyManager
from ..utils.impersonate import apply_impersonate_headers, build_impersonate_meta
from ..utils.logs import get_default_logger
from ..utils.redis_conf import GetRedis


def _is_gitlab_host(netloc: str) -> bool:
    host = (netloc or "").lower()
    return host == "gitlab.com" or host.startswith("gitlab.") or host.endswith(".gitlab.io")


def get_gitlab_purl(url: str) -> str:
    if not url:
        raise ValueError("empty gitlab url")

    return "pkg:gitlab/" + "/".join(url.strip().strip("/").split("/")[2:])


def normalize_gitlab_task(raw: str) -> str:
    """Redis 任务可能是 https://... 或 pkg:gitlab/...（中间件失败重入队）。"""
    from urllib.parse import unquote

    text = (raw or "").strip()
    if not text:
        raise ValueError("empty gitlab task")

    if text.startswith("pkg:gitlab/"):
        path = unquote(text[len("pkg:gitlab/") :]).strip("/")
        if not path:
            raise ValueError(f"invalid gitlab purl: {text}")
        parts = [p for p in path.split("/") if p]
        if parts and "." in parts[0]:
            host, *rest = parts
            if not rest:
                raise ValueError(f"invalid gitlab purl path: {text}")
            return f"https://{host}/{'/'.join(rest)}"
        return f"https://gitlab.com/{path}"

    if text.startswith("http://") or text.startswith("https://"):
        parsed = urllib.parse.urlparse(text)
        if not _is_gitlab_host(parsed.netloc):
            raise ValueError(f"unsupported gitlab url host: {parsed.netloc}")
        path = parsed.path.rstrip("/")
        return f"https://{parsed.netloc.lower()}{path}"

    raise ValueError(f"unsupported gitlab task: {text}")


def gitlab_file_from_url(url: str) -> str | None:
    """与 MinIO 路径规则一致：host 之后的路径 + detail.html。"""
    parsed = urllib.parse.urlparse(url)
    if not _is_gitlab_host(parsed.netloc):
        return None
    path = parsed.path.strip("/")
    if not path:
        return None
    return f"{path}/detail.html"


def is_gitlab_challenge_shell(text: str) -> bool:
    if not text:
        return True
    lower = text.lower()
    return (
        "just a moment" in lower
        or "cf-mitigated" in lower
        or "challenge-platform" in lower
        or "checking your browser" in lower
    )


def is_valid_gitlab_html(text: str) -> bool:
    """过滤 Cloudflare / 空壳页等非目标 HTML。"""
    if not text or len(text) < 500 or is_gitlab_challenge_shell(text):
        return False
    lower = text.lower()
    return "gitlab" in lower and (
        "project-title" in lower
        or "js-project-home-panel" in lower
        or "data-page=" in lower
        or 'content="gitlab' in lower
        or "shortcuts-project" in lower
    )


class GitlabHtmlSpider(RedisSpider):
    name = "gitlab_html"
    redis_key = "gitlab_html:urls"
    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 16,
        "DOWNLOAD_DELAY": 0.3,
        "DOWNLOAD_TIMEOUT": 15,
        "RETRY_TIMES": 1,
        "RETRY_TIMES_403": 1,
        "HTTPERROR_ALLOWED_CODES": [
            404, 403, 405, 429, 500, 502, 503, 504, 408, 400, 202, 451, 417,
        ],
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
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }
        self.http_proxy = PROXY_GITHUB_PYPI
        self._redis_client = None
        self.log_tool = get_default_logger(name="gitlab_html_spider", log_dir="app_logs", max_file_mb=50)

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
            url = normalize_gitlab_task(raw)
            purl = get_gitlab_purl(url) if raw.startswith("http") else raw
            headers, impersonate = apply_impersonate_headers(self.headers)
            meta = build_impersonate_meta(
                {"url": url, "proxy": self.http_proxy, "purl": purl},
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
            self.log_tool.error(f"跳过无效 GitLab 任务 {data!r}: {exc}")
            self.redis_client.lpush(
                self.redis_keys.other_url_key,
                data.decode("utf-8", errors="replace"),
            )
        except Exception as exc:
            self.log_tool.error(f"make_request_from_data error: {exc}")

    def _yield_gitlab_item(self, url, purl, file_name, html, latency=None):
        item = {
            "info": "gitlab_html",
            "url": url,
            "purl": purl,
            "name": file_name,
            "html": html,
        }
        if latency is not None:
            item["latency"] = latency
        return item

    def _record_blocked_response(self, url, status):
        self.log_tool.warning(f"GitLab 无效响应 status={status}: {url}")
        self.redis_client.lpush(
            self.redis_keys.other_code_key,
            json.dumps({"url": url, "status": status}),
        )

    def parse_index(self, response: HtmlResponse):
        try:
            url = response.meta["url"]
            purl = response.meta.get("purl", url)
            file_name = gitlab_file_from_url(url)
            latency = response.meta.get("download_latency", 0)

            if file_name is None:
                self.log_tool.warning(f"无法解析 GitLab 路径: {url}")
                return

            if response.status == 200:
                html_tree = html.fromstring(response.text)
                readme_path = html_tree.xpath(
                    '(//a[@class="nav-link stat-link !gl-px-0 !gl-pb-2 btn-default"])[1]/@href'
                )
                if readme_path:
                    temp_path = readme_path[0]
                    branch_name = temp_path.split("/-/blob/")[-1].strip("/").split("/")[0]
                    readme_url = f"{url}/-/blob/{branch_name}/README.md?ref_type=heads&format=json&viewer=rich"
                    readme_headers = dict(self.headers)
                    readme_headers["accept"] = "application/json, text/plain, */*"
                    readme_headers, impersonate = apply_impersonate_headers(readme_headers)
                    readme_meta = build_impersonate_meta(
                        {
                            "url": url,
                            "proxy": self.http_proxy,
                            "purl": purl,
                            "file_name": file_name,
                        },
                        impersonate=impersonate,
                    )
                    yield scrapy.Request(
                        readme_url,
                        headers=readme_headers,
                        callback=self.parse_readme,
                        errback=self.errback_skip,
                        meta=readme_meta,
                        dont_filter=True,
                    )
                if not is_valid_gitlab_html(response.text):
                    self._record_blocked_response(url, response.status)
                    return
                self.log_tool.info(f"成功 {url} 耗时 {latency:.2f}s")
                yield self._yield_gitlab_item(url, purl, file_name, response.text, latency)
            elif response.status == 404:
                yield self._yield_gitlab_item(url, purl, file_name, "404")
            elif response.status in (403, 405, 451):
                self._record_blocked_response(url, response.status)
            else:
                self.log_tool.warning(f"其他状态码 {response.status}: {url}")
        except IgnoreRequest:
            raise
        except Exception as exc:
            self.log_tool.error(f"parse_index error: {exc}")

    def parse_readme(self, response: HtmlResponse):
        try:
            url = response.meta["url"]
            purl = response.meta.get("purl", url)
            file_name = response.meta.get("file_name") or gitlab_file_from_url(url)
            if not file_name:
                self.log_tool.warning(f"README 无法解析路径: {url}")
                return

            if response.status != 200:
                self.log_tool.warning(f"README 非 200 status={response.status}: {response.url}")
                return

            json_data = json.loads(response.text)
            json_name = file_name.replace(".html", ".json")
            self.log_tool.info(f"README JSON 成功 {url} -> {json_name}")
            yield self._yield_gitlab_item(url, purl, json_name, json.dumps(json_data, ensure_ascii=False))
        except Exception as exc:
            self.log_tool.error(f"parse_readme error: {exc}")


if __name__ == "__main__":
    cmdline.execute("scrapy crawl gitlab_html".split())
