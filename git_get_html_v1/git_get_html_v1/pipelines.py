from twisted.internet import threads

from . import files_async
from .files_async import PutResult
from .utils.create_redis_key import RedisKeyManager
from .utils.minute_stats import inc_success
from .utils.redis_conf import GetRedis
from .settings import REDIS_CONFIG
from . import files


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
    "go_html": "golang-new",
    "nuget_html": "nuget-new",
}


# class GitGetHtmlPipeline:
#     def __init__(self):
#         self.redis_client = GetRedis().redis_client(
#             host=REDIS_CONFIG["host"],
#             port=REDIS_CONFIG["port"],
#             db=REDIS_CONFIG["db"],
#         )

#     def process_item(self, item, spider):
#         bucket = _BUCKET_BY_INFO.get(item.get("info"))
#         if not bucket:
#             return item

#         self.redis_key = RedisKeyManager(spider.name)
#         # d = threads.deferToThread(
#         #     files_async.minio_client_61.put_file_with_retry,
#         #     bucket,
#         #     item["name"],
#         #     item["html"],
#         # )
#         print(files_async.minio_client_61.config)
#         d = threads.deferToThread(
#             files_async.minio_client_61.put_file_with_retry,
#             'golang-2026',
#             'test/test-dd/last.html',
#             'dsdwdwdd',
#         )
#         # minio_client_61.put_file('golang-new', 'test/test/last.html', 'test-1564156486')
#         d.addCallback(self._on_minio_saved, item, spider)
#         d.addErrback(self._on_minio_error, item, spider)
#         return d

#     def _on_minio_saved(self, result, item, spider):
#         label = item["info"].replace("_html", "")
#         if isinstance(result, PutResult) and result.ok:
#             inc_success()
#             self.redis_client.lrem(self.redis_key.run_key, 0, item["url"])
#             self.redis_client.lpush(self.redis_key.success_key, item["purl"])
#             spider.logger.info(f"{label} html 保存成功: {item['name']}")
#         else:
#             reason = result.summary if isinstance(result, PutResult) else "写入失败(已重试)"
#             self._record_minio_failure(item, spider, reason)
#         return item

#     def _on_minio_error(self, failure, item, spider):
#         self._record_minio_failure(item, spider, str(failure.value))
#         return item

#     def _record_minio_failure(self, item, spider, reason):
#         spider.logger.error(f"MinIO 保存失败 {item['name']}: {reason}")
#         payload = item.get("purl") or item["url"]
#         if reason.startswith("AccessDenied"):
#             spider.logger.error(
#                 f"MinIO 权限不足，请检查账号对桶的 PutObject 权限: {_BUCKET_BY_INFO.get(item.get('info'), '?')}"
#             )
#         self.redis_client.lpush(self.redis_key.minio_write_error_key, payload)




class GitGetHtmlPipeline:
    def __init__(self):
        self.redis_client = GetRedis().redis_client(host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'],
                                                    db=REDIS_CONFIG['db'])
        # self.run_key = 'github_html:run_urls'

    def process_item(self, item, spider):
        save_success = None
        self.redis_key = RedisKeyManager(spider.name)
        if item['info'] == 'github_html':
            files.minio_client_61.put_file('github-new', item['name'], item['html'])
            print(f'{item["info"]}:{item["url"]}, {item["name"]} 页面保存成功-------')
            # print(item['html'])
            # with open('github_success.text', 'a', encoding='utf-8') as f:
            #     f.write(item['url'] + '\n')
            #     print('github_url保存成功-------', item['url'])
            save_success = True
        elif item['info'] == 'npm_html':
            # 61
            files.minio_client_61.put_file('npm', item['name'], item['html'])
            print('页面保存成功-------', item['name'])
            # with open('npm_success.text', 'a', encoding='utf-8') as f:
            #     f.write(item['url'] + '\n')
            print(f'{item["info"]}:{item["url"]}, {item["name"]} 页面保存成功-------')
            save_success = True
        elif item['info'] == 'pypi_html':
            files.minio_client_61.put_file('pypi-new', item['name'], item['html'])
            print(f'{item["info"]}:{item["url"]}, {item["name"]} 页面保存成功-------')
            save_success = True
        elif item['info'] == 'go_html':
            files.minio_client_61.put_file('golang-2026', item['name'], item['html'])
            print(f'{item["info"]}:{item["url"]}, {item["name"]} 页面保存成功-------')
            save_success = True
        elif item['info'] == 'nuget_html':
            files.minio_client_61.put_file('nuget-new', item['name'], item['html'])
            print(f'{item["info"]}:{item["url"]}, {item["name"]} 页面保存成功-------')
            save_success = True
        if save_success:
            # 从run_urls中移除url
            self.redis_client.lrem(self.redis_key.run_key, 0, item['url'])
            # 写入到success_urls
            self.redis_client.lpush(self.redis_key.success_key, item.get('purl') or item['url'])

        return item
