# Scrapy settings for get_maven_html project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

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

BOT_NAME = "get_maven_html"

SPIDER_MODULES = ["get_maven_html.spiders"]
NEWSPIDER_MODULE = "get_maven_html.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "get_maven_html (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False
HTTPERROR_ALLOWED_CODES = [404, 429, 500, 502, 503, 504, 408, 400, 202, 403]
RETRY_TIMES = 1
DOWNLOAD_TIMEOUT = 15
# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 10

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "get_maven_html.middlewares.GetMavenHtmlSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   "get_maven_html.middlewares.GetMavenHtmlDownloaderMiddleware": 543,
'scrapy.downloadermiddlewares.retry.RetryMiddleware': None
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   "get_maven_html.pipelines.GetMavenHtmlPipeline": 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
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
REDIS_URL = "redis://47.239.232.1:16379/1"
SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.LifoQueue"
# 配置169  mysql数据库
MYSQL_CONFIG = {
    'host': '8.217.214.169',
    'port': 13306,
    'user': 'Jude',
    'password': 'Super!*6data',
    'database': 'crawler_new'
}
# 配置本地redis
REDIS_CONFIG = {
    'host': '47.239.232.1',
    'port': 16379,
    'db': 1
}
