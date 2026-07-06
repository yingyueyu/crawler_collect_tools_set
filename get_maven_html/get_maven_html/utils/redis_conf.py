import redis
from .logs import EnhancedLogger

# 用来存放获取版本 参与者 语言 等数据的url
redis_client_db0_get_data = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
}
# 专门存放获取html数据的url
redis_client_db1_get_data = {
    'host': 'localhost',
    'port': 6379,
    'db': 1,
}
# 用于存放代理ip
redis_client_db2_get_html = {
    'host': 'localhost',
    'port': 6379,
    'db': 2,
}
# 用来临时存放数据为后续判断数据的完整性使用
redis_client_db3_if_data = {
    'host': 'localhost',
    'port': 6379,
    'db': 3,
}
class GetRedis(object):

    def __init__(self):
        self.log_tool = EnhancedLogger.get_logger(
            module_name="redis_conf",
            log_dir="app_logs",
            rotation="50 MB"
        )

    def redis_client(self, host=None, port=None, db=None, password=None):
        try:
            if password is not None:
                redis_client = redis.Redis(host=host, port=port,
                                           db=db, password=password)
                return redis_client
            else:
                redis_client = redis.Redis(host=host, port=port,
                                           db=db)
                return redis_client

        except Exception as e:
            self.log_tool.error(e)

    # def redis_client_db1(self):
    #     try:
    #         redis_client = redis.Redis(host=redis_client_db1_get_data['host'], port=redis_client_db1_get_data['port'],
    #                                    db=redis_client_db1_get_data['db'])
    #         return redis_client
    #     except Exception as e:
    #         self.log_tool.error(e)
    # def redis_client_db2(self):
    #     redis_client = redis.Redis(host=redis_client_db2_get_html['host'], port=redis_client_db2_get_html['port'],
    #                                db=redis_client_db2_get_html['db'])
    #     return redis_client
    #
    # def redis_client_db3(self):
    #     redis_client = redis.Redis(host=redis_client_db3_if_data['host'], port=redis_client_db3_if_data['port'],
    #                                db=redis_client_db3_if_data['db'])
    #     return redis_client