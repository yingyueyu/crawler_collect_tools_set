# mysql_conf.py

import time
import socket
from pymysql import OperationalError
from socket import timeout as socket_timeout

import pymysql
from dbutils.pooled_db import PooledDB
from pymysql.cursors import DictCursor

from .logs import get_default_logger


from tools.key_token_config import (
    MYSQL_CRAWL_004,
    MYSQL_GITHUB_VER_INFO,
    MYSQL_LOCALHOST,
)

Mysql_github_ver_info = MYSQL_GITHUB_VER_INFO
Mysql_crawl_004 = MYSQL_CRAWL_004
Mysql_localhost = MYSQL_LOCALHOST


class GetMysql(object):
    def __init__(self, client_name):
        self.github_ver_info_pool = None
        self.logger = get_default_logger(
            name="mysql_conf",
            log_dir="app_logs",
            max_file_mb=50,
        )
        self.client_name = client_name
        self._initialized = False

    def initialize_pool(self, retries=3, delay=5):
        """
        延迟初始化连接池，支持重试机制
        """
        for attempt in range(1, retries + 1):
            try:
                self.github_ver_info_pool = PooledDB(
                    creator=pymysql,
                    host=self.client_name['host'],
                    port=int(self.client_name['port']),
                    user=self.client_name['user'],
                    password=self.client_name['password'],
                    db=self.client_name['database'],
                    maxconnections=50,
                    mincached=0,  # 初始化时不预创建连接
                    maxcached=5,
                    blocking=False,  # 连接池无可用连接时立即报错
                    cursorclass=DictCursor,
                    charset='utf8mb4',
                    connect_timeout=5,  # 设置连接超时时间
                )
                self._initialized = True
                self.logger.info(f"数据库连接池初始化成功！")
                return
            except (OperationalError, ConnectionRefusedError, OSError, socket.timeout, socket_timeout) as err:
                self.logger.error(f"第{attempt}次初始化连接池失败: {err}")
                if attempt < retries:
                    self.logger.info(f"{delay}秒后重试...")
                    time.sleep(delay)
                else:
                    self.logger.critical("已达到最大重试次数，放弃连接池初始化")
                    print("[FATAL] 无法初始化数据库连接池，请检查网络、防火墙或数据库状态。")
                    self._initialized = False
                    return
            except Exception as err:
                self.logger.error(f"未知错误: {err}")
                return

    def get_conn(self, retries=3, delay=5):
        if not self._initialized:
            self.logger.warning("连接池未初始化，尝试重新初始化...")
            self.initialize_pool(retries=retries, delay=delay)

        if not self.github_ver_info_pool:
            self.logger.error("连接池初始化失败，无法获取连接")
            return None, None

        for attempt in range(1, retries + 1):
            try:
                conn = self.github_ver_info_pool.connection()
                cursor = conn.cursor()
                self.logger.info(f'数据库{self.client_name["host"]}链接成功！！！！')
                return conn, cursor
            except (OperationalError, ConnectionRefusedError, OSError, socket.timeout, socket_timeout) as err:
                self.logger.error(f"第{attempt}次连接数据库失败: {err}")
                if attempt < retries:
                    self.logger.info(f"{delay}秒后重试...")
                    time.sleep(delay)
                else:
                    self.logger.critical("已达到最大重试次数，放弃连接")
                    print("[FATAL] 无法连接到数据库，请检查网络、防火墙或数据库状态。")
                    return None, None
            except Exception as err:
                self.logger.error(f"未知错误: {err}")
                return None, None

    def close(self, conn, cursor):
        try:
            if conn:
                conn.close()
            if cursor:
                cursor.close()
            self.logger.info(f'数据库{self.client_name["host"]}链接关闭！！！！')
        except Exception as err:
            self.logger.error(f'数据库{self.client_name["host"]}链接关闭失败！！！！{err}')


# if __name__ == '__main__':
#     mysql_client = GetMysql()
#     conn, cursor = mysql_client.get_conn()
#
#     if conn and cursor:
#         try:
#             cursor.execute("SELECT 1")
#             result = cursor.fetchone()
#             print("查询结果:", result)
#         finally:
#             mysql_client.close(conn, cursor)
#     else:
#         print("未能建立数据库连接，程序继续运行但跳过数据库操作。")
