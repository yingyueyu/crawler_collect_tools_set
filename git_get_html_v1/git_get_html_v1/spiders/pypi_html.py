import hashlib
import json
import re

import redis
import scrapy
from scrapy import cmdline
from scrapy.http import HtmlResponse, JsonRequest
from scrapy_redis.spiders import RedisSpider

from ..settings import REDIS_CONFIG
from tools.key_token_config import PROXY_GITHUB_PYPI
from ..utils.impersonate import apply_impersonate_headers, build_impersonate_meta
from ..utils.logs import get_default_logger


class PypiHtmlSpider(RedisSpider):
    name = "pypi_html"
    allowed_domains = ["pypi.org"]
    redis_key = "pypi_html:urls"
    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOAD_DELAY": 0.5,
        "DOWNLOAD_TIMEOUT": 30,
        "RETRY_TIMES": 3,
    }

    def __init__(self):
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "max-age=0",
            "upgrade-insecure-requests": "1",
        }
        self.redis_client_db = redis.Redis(
            host=REDIS_CONFIG["host"],
            port=REDIS_CONFIG["port"],
            db=REDIS_CONFIG["db"],
        )
        self.log_tool = get_default_logger(
            name="pypi_html_spider",
            log_dir="app_logs",
            max_file_mb=50,
        )
        self.http_proxy = PROXY_GITHUB_PYPI

    def _request_meta(self, url, proxy=None):
        proxy = proxy or self.http_proxy
        return build_impersonate_meta({"url": url, "proxy": proxy, "download_timeout": 30})

    def make_request_from_data(self, data):
        try:
            url = data.decode("utf-8")
            if url.startswith("https://"):
                url = url if url[-1] != "/" else url[0:-1]
            elif url.startswith("pkg:pypi"):
                url = "https://pypi.org/project/" + url.replace("pkg:pypi/", "")
            else:
                url = "https://pypi.org/project/" + url

            headers, _ = apply_impersonate_headers(self.headers)
            yield scrapy.Request(
                url,
                headers=headers,
                callback=self.parse_index,
                meta=self._request_meta(url),
                dont_filter=True,
            )
        except Exception as e:
            self.log_tool.error(f"{e} {data}")

    def json_parse(self, input_string):
        json_part, *other_parts = input_string.split(", ", 1)
        json_part = json_part.strip()
        json_data = json.loads(json_part)
        other_parts = other_parts[0].split(", ")
        parsed_others = []
        for part in other_parts:
            part = part.strip()
            if part.lower() == "true":
                parsed_others.append(True)
            elif part.lower() == "false":
                parsed_others.append(False)
            elif part.startswith('"') and part.endswith('"'):
                parsed_others.append(part[1:-1])
            else:
                parsed_others.append(part)
        return {
            "json_part": json_data,
            "encrypted_str": parsed_others[0],
            "path": parsed_others[1],
            "flag": parsed_others[2],
        }

    def generate_answer(self, base, target_hash):
        charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        for char1 in charset:
            for char2 in charset:
                candidate = base + char1 + char2
                if hashlib.sha256(candidate.encode()).hexdigest() == target_hash:
                    return char1 + char2
        return ""

    def parse(self, response: HtmlResponse, **kwargs):
        try:
            if response.status != 200:
                return
            pattern = r"init(\(.*?\));"
            matches = re.findall(pattern, response.text, re.DOTALL)
            if not matches:
                return

            init_params = matches[-1]
            input_string = init_params.strip("()")
            structured_data = self.json_parse(input_string)
            post_url = "https://pypi.org/_fs-ch-1T1wmsGaOgGaSxcX/fst-post-back"
            _data = structured_data["json_part"][0].get("data")
            data = {
                "token": structured_data["encrypted_str"],
                "data": [{
                    "ty": "pow",
                    "base": _data.get("base"),
                    "answer": self.generate_answer(_data.get("base"), _data.get("hash")),
                    "hmac": _data.get("hmac"),
                    "expires": _data.get("expires"),
                }],
            }
            _headers = {
                "accept": "application/json",
                "accept-language": "zh-CN,zh;q=0.9",
                "cache-control": "no-cache",
                "content-type": "application/json",
                "origin": "https://pypi.org",
                "pragma": "no-cache",
                "priority": "u=1, i",
                "referer": response.meta["url"],
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            }
            _headers, _ = apply_impersonate_headers(_headers, response.meta.get("impersonate"))
            meta = build_impersonate_meta(
                {
                    "proxy": response.meta["proxy"],
                    "url": response.meta["url"],
                    "set_cookie": response.meta["set_cookie"],
                },
                impersonate=response.meta.get("impersonate"),
            )
            yield JsonRequest(
                url=post_url,
                headers=_headers,
                callback=self.req_index,
                data=data,
                meta=meta,
                dont_filter=True,
            )
        except Exception as e:
            self.log_tool.error(f"数据解析失败{e} {response.meta['url']}")

    def req_index(self, response):
        try:
            set_cookie_header = response.headers[b"Set-Cookie"]
            if not set_cookie_header:
                return
            set_cookie_ = re.findall("b'(.*?);", str(set_cookie_header))[0]
            headers, _ = apply_impersonate_headers(self.headers, response.meta.get("impersonate"))
            headers["cookie"] = set_cookie_
            headers["referer"] = response.meta["url"]
            meta = build_impersonate_meta(
                {
                    "url": response.meta["url"],
                    "proxy": response.meta["proxy"],
                    "set_cookie": response.meta["set_cookie"],
                },
                impersonate=response.meta.get("impersonate"),
            )
            yield scrapy.Request(
                url=response.meta["url"],
                headers=headers,
                callback=self.parse_index,
                meta=meta,
                dont_filter=True,
            )
        except Exception as e:
            self.redis_client_db.lpush("pypi_html:urls", response.meta["url"])
            self.log_tool.error(f"请求页面出错{e} {response.meta['url']}")

    def parse_index(self, response: HtmlResponse):
        try:
            module = response.meta["url"].split("project/")[-1].strip("/")
            file = f"{module}/last.html"
            blocked = "A required part of this site couldn" in response.text
            if response.status == 200 and not blocked:
                yield {
                    "info": "pypi_html",
                    "url": response.meta["url"],
                    "name": file,
                    "html": response.text,
                }
            elif response.status == 404:
                yield {
                    "info": "pypi_html",
                    "url": response.meta["url"],
                    "name": file,
                    "html": "404",
                }
            else:
                self.log_tool.warning(f"其他状态码 {response.status}: {response.meta['url']}")
        except Exception as e:
            self.log_tool.error(f"解析页面出错{e} {response.meta['url']}")


if __name__ == "__main__":
    cmdline.execute("scrapy crawl pypi_html".split())
