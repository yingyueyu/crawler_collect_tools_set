import scrapy
from scrapy import cmdline
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from scrapy_redis.spiders import RedisSpider

from tools.key_token_config import PROXY_GITHUB_PYPI
from ..utils.create_redis_key import RedisKeyManager
from ..utils.impersonate import apply_impersonate_headers, build_impersonate_meta, is_valid_pypi_html
from ..utils.logs import get_default_logger
from ..utils.pypi_cf_fetch import normalize_pypi_url
from ..utils.redis_conf import GetRedis


def pypi_module_from_url(url: str) -> str:
    return url.split("project/")[-1].strip("/")


class PypiHtmlSpider(RedisSpider):
    name = "pypi_html"
    allowed_domains = ["pypi.org"]
    redis_key = "pypi_html:urls"
    custom_settings = {
        "CONCURRENT_REQUESTS": 10,
        "DOWNLOAD_DELAY": 0.5,
        "DOWNLOAD_TIMEOUT": 60,
        "RETRY_TIMES": 3,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "max-age=0",
            "upgrade-insecure-requests": "1",
        }
        self.http_proxy = PROXY_GITHUB_PYPI
        self._redis_client = None
        self.log_tool = get_default_logger(
            name="pypi_html_spider",
            log_dir="app_logs",
            max_file_mb=50,
        )

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
        url = failure.request.meta.get("url", failure.request.url)
        self.log_tool.warning(f"请求失败跳过: {url} ({failure.value})")
        self.redis_client.lpush(self.redis_keys.error_key, url)

    def _request_meta(self, url, raw, impersonate=None):
        return build_impersonate_meta(
            {
                "url": url,
                "raw": raw,
                "proxy": self.http_proxy,
                "download_timeout": 60,
                "pypi_cf_handled": True,
            },
            impersonate=impersonate,
        )

    def make_request_from_data(self, data):
        try:
            raw = data.decode("utf-8").strip()
            url = normalize_pypi_url(raw)
            headers, impersonate = apply_impersonate_headers(self.headers)
            yield scrapy.Request(
                url,
                headers=headers,
                callback=self.parse_index,
                errback=self.errback_skip,
                meta=self._request_meta(url, raw, impersonate),
                dont_filter=True,
            )
        except ValueError as exc:
            self.log_tool.error(f"跳过无效 PyPI 任务 {data!r}: {exc}")
            self.redis_client.lpush(
                self.redis_keys.other_url_key,
                data.decode("utf-8", errors="replace"),
            )
        except Exception as exc:
            self.log_tool.error(f"make_request_from_data error: {exc} {data!r}")

    def parse_index(self, response: HtmlResponse):
        try:
            url = response.meta["url"]
            raw = response.meta.get("raw", url)
            module = pypi_module_from_url(url)
            file_name = f"{module}/last.html"
            latency = response.meta.get("download_latency", 0)

            if response.status == 404:
                yield {
                    "info": "pypi_html",
                    "url": url,
                    "purl": raw,
                    "name": file_name,
                    "html": "404",
                }
                return

            if response.status == 200 and is_valid_pypi_html(response.text):
                self.log_tool.info(f"成功 {url} 耗时 {latency:.2f}s")
                yield {
                    "info": "pypi_html",
                    "url": url,
                    "purl": raw,
                    "name": file_name,
                    "html": response.text,
                    "latency": latency,
                }
                return

            self.log_tool.warning(
                f"过盾失败 status={response.status}, len={len(response.text or '')}: {url}"
            )
            self.redis_client.lpush(self.redis_keys.forbidden_key, url)
        except IgnoreRequest:
            raise
        except Exception as exc:
            self.log_tool.error(f"parse_index error: {exc} {response.meta.get('url')}")


if __name__ == "__main__":
    cmdline.execute("scrapy crawl pypi_html".split())
