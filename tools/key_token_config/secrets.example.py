"""
密钥 / Token 配置模板。

使用方式：
  1. 复制本文件为 secrets.py（secrets.py 已被 .gitignore 忽略）
  2. 填写真实值
  3. 业务代码：from tools.key_token_config import PROXY_DEFAULT
"""

# ---------------------------------------------------------------------------
# HTTP 代理
# ---------------------------------------------------------------------------
PROXY_DEFAULT = "http://user:password@proxy.example.com:33923"
PROXY_GITHUB_PYPI = "http://user:password@proxy.example.com:31923"
PROXY_NUGET = "http://user:password@proxy.example.com:32923"

# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------
MINIO_LOCAL_TEST = {
    "url": "127.0.0.1:9000",
    "accessKey": "minioadmin",
    "secretKey": "minioadmin",
    "api": "s3v4",
    "path": "auto",
    "bucket": "test",
}

MINIO_DEFAULT = {
    "url": "minio.example.com:9000",
    "accessKey": "your-access-key",
    "secretKey": "your-secret-key",
    "api": "s3v4",
    "path": "auto",
}

MINIO_99 = {
    "url": "192.168.0.99:19000",
    "accessKey": "your-access-key",
    "secretKey": "your-secret-key",
    "api": "s3v4",
    "path": "auto",
}

MINIO_168 = {
    "url": "10.10.0.168:19000",
    "accessKey": "your-access-key",
    "secretKey": "your-secret-key",
    "api": "s3v4",
    "path": "auto",
}

MINIO_12 = {
    "url": "61.170.32.12:19000",
    "accessKey": "your-access-key",
    "secretKey": "your-secret-key",
    "api": "s3v4",
    "path": "auto",
}

MINIO_61_TEST = {
    "url": "61.170.32.12:19000",
    "accessKey": "your-access-key",
    "secretKey": "your-secret-key",
    "api": "s3v4",
    "path": "auto",
}

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
REDIS_GIT_GET_HTML = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 1,
}

REDIS_GIT_GET_HTML_URL = "redis://127.0.0.1:6379/1"

REDIS_LOCAL = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 1,
}

REDIS_LOCAL_URL = "redis://127.0.0.1:6379/1"

REDIS_TOOLS_DEFAULT_HOST = "127.0.0.1"

# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------
MYSQL_GITHUB_VER_INFO = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "your-password",
    "database": "your_database",
}

MYSQL_CRAWL_004 = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "your-password",
    "database": "maven_data",
}

MYSQL_LOCALHOST = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "your-password",
    "database": "maven_api_data",
}

MYSQL_TOOLS_DEFAULT = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "your-password",
    "database": "api_data",
}

# ---------------------------------------------------------------------------
# GitHub
# ---------------------------------------------------------------------------
GITHUB_TOKENS = [
    "your_github_token_here",
]

# ---------------------------------------------------------------------------
# 快代理 API
# ---------------------------------------------------------------------------
KDL_PROXY_API_URL = (
    "https://dps.kdlapi.com/api/getdps/"
    "?secret_id=YOUR_SECRET_ID"
    "&signature=YOUR_SIGNATURE"
    "&num=5&format=json&sep=1"
)
