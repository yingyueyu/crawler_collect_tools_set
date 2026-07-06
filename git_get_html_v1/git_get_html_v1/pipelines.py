from twisted.internet import threads

from . import files_async
from .files_async import PutResult
from .utils.create_redis_key import RedisKeyManager
from .utils.redis_conf import GetRedis
from .settings import REDIS_CONFIG


def npm_url_to_purl(url: str) -> str:
    import re
    from urllib.parse import unquote

    url = (url or "").strip()
    if url.startswith("pkg:npm/"):
        return url
    match = re.match(r"https?://www\.npmjs\.com/package/(.+)", url, re.I)
    if match:
        return f"pkg:npm/{unquote(match.group(1))}"
    return url


_BUCKET_BY_INFO = {
    "github_html": "github-new",
    "npm_html": "npm",
    "pypi_html": "pypi-new",
    "go_html": "golang-2026",
    "nuget_html": "nuget-new",
    "maven_html": "mvn-2026",
}


class GitGetHtmlPipeline:
    def __init__(self):
        self.redis_client = GetRedis().redis_client(
            host=REDIS_CONFIG["host"],
            port=REDIS_CONFIG["port"],
            db=REDIS_CONFIG["db"],
        )

    def process_item(self, item, spider):
        bucket = _BUCKET_BY_INFO.get(item.get("info"))
        if not bucket:
            return item

        self.redis_key = RedisKeyManager(spider.name)
        d = threads.deferToThread(
            files_async.minio_client_61.put_file_with_retry,
            bucket,
            item["name"],
            item["html"],
        )
        d.addCallback(self._on_minio_saved, item, spider)
        d.addErrback(self._on_minio_error, item, spider)
        return d

    def _on_minio_saved(self, result, item, spider):
        label = item["info"].replace("_html", "")
        if isinstance(result, PutResult) and result.ok:
            print(f'{item["info"]}:{item["url"]}, {item["name"]} 页面保存成功-------')
            if item["info"] == "maven_html":
                self._record_maven_success(item, spider)
            else:
                self.redis_client.lrem(self.redis_key.run_key, 0, item["url"])
                self.redis_client.lpush(self.redis_key.success_key, item.get("purl") or item["url"])
            spider.logger.info(f"{label} html 保存成功: {item['name']}")
        else:
            reason = result.summary if isinstance(result, PutResult) else "写入失败"
            self._record_minio_failure(item, spider, reason)
        return item

    def _record_maven_success(self, item, spider):
        """与 get_maven_html pipeline 一致：success 写 url，run_key 按 purl 移除。"""
        if item.get("html") == "404":
            self.redis_client.lpush(self.redis_key.req_404_key, item["url"])
        self.redis_client.lpush(self.redis_key.success_key, item["url"])
        self.redis_client.lrem(self.redis_key.run_key, 0, item["purl"])

    def _on_minio_error(self, failure, item, spider):
        self._record_minio_failure(item, spider, str(failure.value))
        return item

    def _record_minio_failure(self, item, spider, reason):
        spider.logger.error(f"MinIO 保存失败 {item['name']}: {reason}")
        payload = item.get("purl") or item["url"]
        if reason.startswith("AccessDenied"):
            spider.logger.error(
                f"MinIO 权限不足，请检查账号对桶的 PutObject 权限: "
                f"{_BUCKET_BY_INFO.get(item.get('info'), '?')}"
            )
        self.redis_client.lpush(self.redis_key.minio_write_error_key, payload)
