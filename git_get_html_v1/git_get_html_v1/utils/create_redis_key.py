
class RedisKeyManager:
    def __init__(self, spider_name: str):
        self.spider_name = spider_name

    @property
    def timeout_key(self):
        return f"{self.spider_name}:timeout_urls"

    @property
    def run_key(self):
        return f"{self.spider_name}:run_urls"

    @property
    def other_code_key(self):
        return f"{self.spider_name}:other_code_urls"

    @property
    def req_404_key(self):
        return f"{self.spider_name}:req_404_urls"

    @property
    def forbidden_key(self):
        return f"{self.spider_name}:403_urls"

    @property
    def error_key(self):
        return f"{self.spider_name}:error_urls"

    @property
    def success_key(self):
        return f"{self.spider_name}:success_urls"

    @property
    def index_success_key(self):
        return f"{self.spider_name}:index_success_urls"

    @property
    def first_success_key(self):
        return f"{self.spider_name}:first_success_urls"

    @property
    def other_url_key(self):
        return f"{self.spider_name}:other_urls"

    @property
    def not_version_key(self):
        return f"{self.spider_name}:not_version_urls"

    @property
    def not_Central_Google_key(self):
        return f"{self.spider_name}:not_Central_Google_urls"

    @property
    def old_new_url_key(self):
        return f"{self.spider_name}:old_new_urls"

    @property
    def other_length_key(self):
        return f"{self.spider_name}:other_length_urls"

    @property
    def minio_write_error_key(self):
        return f"{self.spider_name}:minio_write_error_urls"
    # ... 其他属性