from datetime import datetime

from scrapy import signals
from twisted.internet import task

from .utils.logs import get_default_logger
from .utils.minute_stats import pop_minute_report

_logger = get_default_logger(name="minute_stats", log_dir="app_logs", max_file_mb=50)


class MinuteStatsExtension:
    """每分钟输出成功数（含 404 入库）与 403 数。"""

    def __init__(self, interval):
        self.interval = interval
        self._loop = None

    @classmethod
    def from_crawler(cls, crawler):
        interval = crawler.settings.getint("MINUTE_STATS_INTERVAL", 60)
        ext = cls(interval)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        self._loop = task.LoopingCall(self._report, spider)
        self._loop.start(self.interval, now=False)

    def spider_closed(self, spider):
        if self._loop and self._loop.running:
            self._loop.stop()
        self._report(spider, final=True)

    def _report(self, spider, final=False):
        report = pop_minute_report()
        if report["success"] == 0 and report["forbidden_403"] == 0 and not final:
            return
        minute = datetime.now().strftime("%Y-%m-%d %H:%M")
        tag = "最终统计" if final else "分钟统计"
        _logger.info(
            f"[{tag}] {minute} "
            f"成功={report['success']} "
            f"403={report['forbidden_403']}"
        )
