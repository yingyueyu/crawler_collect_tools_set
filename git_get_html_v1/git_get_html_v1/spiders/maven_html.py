import json
import urllib.parse

import scrapy
from scrapy import cmdline
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from scrapy_redis.spiders import RedisSpider

from tools.key_token_config import PROXY_DEFAULT
from ..utils.create_redis_key import RedisKeyManager
from ..utils.impersonate import apply_impersonate_headers, build_impersonate_meta
from ..utils.logs import get_default_logger
from ..utils.redis_conf import GetRedis


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


def get_group_id_and_artifact_id(purl: str) -> tuple[str, str]:
    if not purl or not isinstance(purl, str):
        raise ValueError(f"invalid maven purl: {purl}")
    if purl.startswith("pkg:maven/"):
        parts = purl.strip("/").split("/")
        return parts[1], parts[2]
    if purl.startswith("https://mvnrepository.com/artifact/"):
        parts = purl.strip("/").split("/")
        return parts[3], parts[4]
    raise ValueError(f"invalid maven purl: {purl}")


class MavenHtmlSpider(RedisSpider):
    name = "maven_html"
    allowed_domains = ["mvnrepository.com"]
    redis_key = "maven_html:urls"
    custom_settings = {
        "CONCURRENT_REQUESTS": 10,
        "DOWNLOAD_TIMEOUT": 15,
        "RETRY_TIMES": 1,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "upgrade-insecure-requests": "1",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
        }
        self.http_proxy = PROXY_DEFAULT
        self._redis_client = None
        self.log_tool = get_default_logger(
            name="maven_html_spider",
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
        self.log_tool.warning(f"请求失败跳过: {failure.request.url} ({failure.value})")

    def make_request_from_data(self, data):
        raw = data.decode("utf-8").strip()
        try:
            group_id, artifact_id = get_group_id_and_artifact_id(raw)
            url = normalize_maven_task_url(raw)
            purl = f"pkg:maven/{group_id}/{artifact_id}"
        except ValueError as exc:
            self.log_tool.error(f"跳过无效任务 {raw!r}: {exc}")
            self.redis_client.lpush(self.redis_keys.other_url_key, raw)
            return

        paths = urllib.parse.urlparse(url).path.strip("/").split("/")
        if len(paths) <= 3:
            headers, impersonate = apply_impersonate_headers(self.headers)
            meta = build_impersonate_meta(
                {"url": url, "purl": purl, "proxy": self.http_proxy},
                impersonate=impersonate,
            )
            yield scrapy.Request(
                url,
                headers=headers,
                callback=self.parse_,
                errback=self.errback_skip,
                meta=meta,
                dont_filter=True,
            )
            self.redis_client.lpush(self.redis_keys.run_key, purl)
        else:
            self.redis_client.lpush(self.redis_keys.other_url_key, url)

    def parse_(self, response: HtmlResponse):
        url = response.meta["url"]
        if response.xpath('//li[@class="active"]/a[1]'):
            repo_text = ",".join(response.xpath('//div[@id="snippets"]//li/a/text()').getall())
            if "Central" in repo_text or "Google" in repo_text:
                yield from self.parse_index(response)
            else:
                self.log_tool.debug(f"不是中央仓库或谷歌仓: {url}")
                self.redis_client.lpush(self.redis_keys.not_Central_Google_key, url)

    def parse_index(self, response: HtmlResponse):
        url = response.meta["url"]
        purl = response.meta["purl"]
        group_id, artifact_id = get_group_id_and_artifact_id(url)
        file_name = f"{group_id}/{artifact_id}/versions.html"
        latency = response.meta.get("download_latency", 0)

        try:
            if response.status == 200:
                item = {
                    "info": "maven_html",
                    "url": url,
                    "purl": purl,
                    "name": file_name,
                    "html": response.text,
                    "latency": latency,
                }
                if response.meta.get("new_index_url"):
                    item["url"] = response.meta["new_index_url"]
                    yield item
                    self.redis_client.lpush(
                        self.redis_keys.old_new_url_key,
                        json.dumps({
                            "old_url": response.meta["old_url"],
                            "new_url": response.meta["new_index_url"],
                        }),
                    )
                    self.redis_client.lrem(self.redis_keys.run_key, 0, response.meta["old_url"])
                else:
                    self.log_tool.info(f"成功 {url} 耗时 {latency:.2f}s")
                    yield item
            elif response.status == 404:
                self.log_tool.error(f"404 页面: {url}")
                self.redis_client.lpush(self.redis_keys.req_404_key, url)
                yield {
                    "info": "maven_html",
                    "url": url,
                    "purl": purl,
                    "name": file_name,
                    "html": "404",
                }
        except IgnoreRequest:
            raise
        except Exception as exc:
            self.log_tool.error(f"处理组件页面出错: {url}, 错误: {exc}")


if __name__ == "__main__":
    cmdline.execute("scrapy crawl maven_html".split())
