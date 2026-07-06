# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from .utils.create_redis_key import RedisKeyManager
from .utils.redis_conf import GetRedis
from .utils.logs import EnhancedLogger
from .utils.files import minio_client_12

class GetMavenHtmlPipeline:
    def __init__(self,  settings):
        self.log_tool = EnhancedLogger.get_logger(
            module_name="get_maven_html_pipeline",
            log_dir="app_logs",
            rotation="50 MB"
        )
        redis_config = settings.get('REDIS_CONFIG')
        self.redis_client = GetRedis().redis_client(
            host=redis_config.get('host'),
            port=redis_config.get('port'),
            db=redis_config.get('db')
        )

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler.settings)
        obj.keys = RedisKeyManager(crawler.spider.name)
        return obj

    def process_item(self, item, spider):
        if item['data'] == 'index_html':
            minio_client_12.put_file('mvn-2026', item['filename'], item['html'])
            print(f"{item['url']} -----------> success")
            self.redis_client.lpush(self.keys.success_key, item['url'])
            self.redis_client.lrem(self.keys.run_key, 0, item['purl'])
        # elif item['data'] == 'first_version_html':
        #     print(f"{item['url']} -----------> success")
        #     minio_client_12.put_file('mvn-2026', item['filename'], item['html'])
        #     self.redis_client.lpush(self.keys.success_key, item['url'])
        #     self.redis_client.lrem(self.keys.run_key, 0, item['url'])
        elif item['data'] == 'index_html_404':
            print(f"{item['url']} -----------> 404")
            minio_client_12.put_file('mvn-2026', item['filename'], item['html'])
            self.redis_client.lpush(self.keys.req_404_key, item['url'])
            self.redis_client.lpush(self.keys.success_key, item['url'])
            self.redis_client.lrem(self.keys.run_key, 0, item['purl'])