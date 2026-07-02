import json
import time

import jsonpath
import requests
import redis
from urllib3 import request

from tools.key_token_config import KDL_PROXY_API_URL
from .redis_conf import GetRedis
from ..settings import REDIS_CONFIG


class ProxyRedis:
    def __init__(self):
        self.redis_client = GetRedis().redis_client(
            host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'], db=REDIS_CONFIG['db']
        )

    def get_proxies(self):
        try:
            url = KDL_PROXY_API_URL
            response = requests.get(url)
            if response.status_code == 200:
                # proxy_list = jsonpath.jsonpath(response.json(), '$..server')  # 青果提取逻辑
                proxy_list = jsonpath.jsonpath(response.json(), '$..proxy_list')[0]   # 快代理
                for proxy in proxy_list:
                    proxies = {
                        "http": f"http://{proxy}",
                        "https": f"http://{proxy}",
                    }
                    print(f"获取新代理成功: {proxies}")
                    yield proxies
        except Exception as e:
            print(e)

    def proxy_if(self):
        proxy_ = self.get_proxies()
        for proxy in proxy_:
            try:
                self.redis_client.lpush('proxy_list', json.dumps(proxy))
                print('代理写入成功-----------------')

            except Exception as e:
                self.redis_client.lpush('proxy_443_list', json.dumps(proxy))
                print('超时代理写入成功-------------')
                print(e)


    def is_key_has_value(self, key):
        # 判断键是否存在
        if not self.redis_client.exists(key):
            return False
        # 获取键的类型
        key_type = self.redis_client.type(key).decode('utf-8') if isinstance(self.redis_client.type(key), bytes) else self.redis_client.type(key)
        # 根据类型检查是否有值
        if key_type == 'string':
            return self.redis_client.strlen(key) > 0
        elif key_type == 'list':
            return self.redis_client.llen(key) > 0
        elif key_type == 'hash':
            return self.redis_client.hlen(key) > 0
        elif key_type == 'set':
            return self.redis_client.scard(key) > 0
        elif key_type == 'zset':
            return self.redis_client.zcard(key) > 0
        else:
            return False  # 未知类型默认返回 False

    def get_random_from_list(self, key):
        length = self.redis_client.llen(key)
        if length == 0:
            return None
        import random
        index = random.randint(0, length - 1)
        return self.redis_client.lindex(key, index)

    def get_random_from_hash(self, key):
        fields = self.redis_client.hkeys(key)
        if not fields:
            return None
        import random
        random_field = random.choice(fields)
        return self.redis_client.hget(key, random_field)

    def get_random_from_zset(self, key):
        members = self.redis_client.zrange(key, 0, -1)
        if not members:
            return None
        import random
        return random.choice(members)

    def get_random_char_from_string(self, key):
        value = self.redis_client.get(key)
        if not value:
            return None
        import random
        return random.choice(value)

    def get_random_value(self, key, retries=100):
        """当获取为空时，进行重试"""
        for attempt in range(retries):
            key_type = self.redis_client.type(key).decode('utf-8')

            # 处理键不存在的情况（type 返回 'none'）
            if key_type == 'none':
                print(f"键 {key} 不存在，尝试重试 ({attempt + 1}/{retries})")
                # proxy_if()
                time.sleep(5)  # 减少睡眠时间，避免过长等待
                continue

            # 获取随机值，处理可能的 None 返回
            try:
                if key_type == 'set':
                    value = self.redis_client.srandmember(key)
                elif key_type == 'list':
                    value = self.get_random_from_list(key)
                elif key_type == 'hash':
                    value = self.get_random_from_hash(key)
                elif key_type == 'zset':
                    value = self.get_random_from_zset(key)
                elif key_type == 'string':
                    value = self.get_random_char_from_string(key)
                else:
                    print(f"未知键类型: {key_type}，尝试重试 ({attempt + 1}/{retries})")
                    time.sleep(5)
                    continue

                # 检查是否获取到值
                if value is None:
                    print(f"从 {key_type} 类型的键 {key} 获取到 None 值，尝试重试 ({attempt + 1}/{retries})")
                    time.sleep(5)
                    continue

                return value.decode('utf-8')

            except Exception as e:
                print(f"获取键 {key} 的随机值时出错: {e}，尝试重试 ({attempt + 1}/{retries})")
                time.sleep(5)

        print(f"重试 {retries} 次后仍然无法获取到有效值")
        return None  # 重试次数用尽后返回 None

    def del_proxy(self, proxy):
        self.redis_client.lrem('proxy_list', count=0, value=json.dumps(proxy))
        print(f'代理删除成功----------{proxy}--------')
        self.redis_client.lpush('proxy_done_list', json.dumps(proxy))
        print('失效代理写入成功-------------')

if __name__ == '__main__':
    pr = ProxyRedis()
    print(pr.is_key_has_value('proxy_list'))
    # print(self.redis_client.get('proxy_list'))
    pr.proxy_if()
    proxy = pr.get_random_value('proxy_list')
    print(json.loads(proxy)['http'])
    # pr.del_proxy(proxy)