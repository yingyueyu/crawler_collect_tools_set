# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from .utils.redis_conf import GetRedis
from .utils.logs import get_default_logger
from . import files

class GetNugetHtmlPipeline:
    def __init__(self):
        self.redis_client = GetRedis().redis_client(host='127.0.0.1', port=6379, db=1)
        self.run_key = 'nuget_html:run_urls'
        self.success_key = 'nuget_html:success_urls'
        self.log_tool = get_default_logger(
            name="nuget_html_pipeline",
            log_dir="app_logs",
            max_file_mb=50,
        )
    def process_item(self, item, spider):
        if item['info'] == 'nuget_html':
            print('nuget_html')
            # files.minio_client_99.put_file('github-new', item['name'], item['html'])
            files.minio_client_12.put_file('nuget-new', item['name'], item['html'])
            print('页面html保存成功-----', item['name'])
            # print(item['html'])
            self.redis_client.lpush(self.run_key, item['url'])
            self.redis_client.lrem(self.run_key, 0, item['url'])
            self.redis_client.lpush(self.success_key, item['url'])
            with open('nuget_html.txt', 'a', encoding='utf-8') as f:
                f.write(item['url'] + '\n')
                print('nuget_html保存成功-------', item['url'])
        if item['info'] == 'nuget_html_404':
            print('nuget_html_404')
            self.redis_client.lpush(self.run_key, item['url'])
            self.redis_client.lrem(self.run_key, 0, item['url'])
            with open('nuget_html_404.txt', 'a', encoding='utf-8') as f:
                f.write(item['url'] + '\n')
                print('nuget_html_404保存成功-------', item['url'])