import scrapy
from scrapy import cmdline
from scrapy.http import HtmlResponse
from scrapy_redis.spiders import RedisSpider

from tools.key_token_config import PROXY_GITHUB_PYPI
from ..utils.impersonate import apply_impersonate_headers, build_impersonate_meta
from ..utils.logs import get_default_logger


class GitlabHtmlSpider(RedisSpider):
    name = "gitlab_html"
    allowed_domains = ["gitlab.com"]
    redis_key = "gitlab_html:urls"

    def __init__(self):
        super().__init__()
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
        self.logger = get_default_logger(name="gitlab_html_spider", log_dir="app_logs", max_file_mb=50)

    def make_request_from_data(self, data):
        try:
            url = data.decode("utf-8")
            url = url.lower() if url[-1] != "/" else url[0:-1].lower()
            headers, impersonate = apply_impersonate_headers(self.headers)
            meta = build_impersonate_meta(
                {"url": url, "proxy": self.http_proxy},
                impersonate=impersonate,
            )
            yield scrapy.Request(
                url,
                headers=headers,
                callback=self.parse_index,
                meta=meta,
                dont_filter=True,
            )
        except Exception as e:
            self.logger.error(f"make_request_from_data error: {e}")

    def parse_index(self, response: HtmlResponse, **kwargs):
        yield {
            "info": "gitlab_html",
            "url": response.meta["url"],
            "html": response.text,
        }


if __name__ == "__main__":
    cmdline.execute("scrapy crawl gitlab_html".split())
