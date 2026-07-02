# Scrapy settings for git_get_html_v1 project
#
# 基于 git_get_html，全局启用 scrapy_impersonate + curl_cffi 进行 TLS 指纹伪装

import sys
from pathlib import Path


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "tools" / "key_token_config").is_dir():
            return parent
    raise RuntimeError("cannot find repository root (tools/key_token_config)")


_ROOT = _repo_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

BOT_NAME = "git_get_html_v1"

SPIDER_MODULES = ["git_get_html_v1.spiders"]
NEWSPIDER_MODULE = "git_get_html_v1.spiders"

ROBOTSTXT_OBEY = False

LOG_LEVEL = "INFO"

CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 0.5
RETRY_TIMES = 5
DOWNLOAD_TIMEOUT = 30

# curl_cffi 通过 scrapy_impersonate 接管 HTTP 下载
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
USER_AGENT = None
DOWNLOAD_HANDLERS = {
    "http": "scrapy_impersonate.ImpersonateDownloadHandler",
    "https": "scrapy_impersonate.ImpersonateDownloadHandler",
}

DOWNLOADER_MIDDLEWARES = {
    "git_get_html_v1.middlewares.GitGetHtmlDownloaderMiddleware": 543,
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
}

ITEM_PIPELINES = {
    "git_get_html_v1.pipelines.GitGetHtmlPipeline": 301,
}

EXTENSIONS = {
    "git_get_html_v1.extensions.MinuteStatsExtension": 500,
}

MINUTE_STATS_INTERVAL = 60

HTTPERROR_ALLOWED_CODES = [404, 403, 429, 500, 502, 503, 504, 408, 400, 202, 451, 417]
FEED_EXPORT_ENCODING = "utf-8"

""" scrapy-redis配置 """
SCHEDULER = "scrapy_redis.scheduler.Scheduler"

from scrapy_redis_bloomfilter.dupefilter import RFPDupeFilter

DUPEFILTER_CLASS = "scrapy_redis_bloomfilter.dupefilter.RFPDupeFilter"
BLOOMFILTER_BIT = 30

SCHEDULER_PERSIST = True
REDIS_PARAMS = {
    "socket_keepalive": True,
    "max_connections": 100,
}
from tools.key_token_config import REDIS_GIT_GET_HTML, REDIS_GIT_GET_HTML_URL

REDIS_URL = REDIS_GIT_GET_HTML_URL
SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.LifoQueue"

REDIS_CONFIG = REDIS_GIT_GET_HTML
