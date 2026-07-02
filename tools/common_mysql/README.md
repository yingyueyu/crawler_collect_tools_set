# common_mysql

通用 MySQL 连接与批处理工具，提供同步/异步客户端、连接池、事务与分块批量写入，供标签分类、数据同步等脚本复用。

## 安装依赖

```bash
pip install pymysql aiomysql
```

## 环境变量

通过 `MySQLConfig.from_env()` 读取，默认前缀 `MYSQL_`：

| 变量 | 说明 | 默认 |
|------|------|------|
| `MYSQL_HOST` | 主机 | 见 `config.py` |
| `MYSQL_PORT` | 端口 | `3306` |
| `MYSQL_USER` | 用户名 | — |
| `MYSQL_PASSWORD` | 密码 | — |
| `MYSQL_DATABASE` | 库名 | 空 |
| `MYSQL_CHARSET` | 字符集 | `utf8mb4` |
| `MYSQL_CONNECT_TIMEOUT` | 连接超时（秒） | `10` |
| `MYSQL_READ_TIMEOUT` | 读超时（秒） | `300` |
| `MYSQL_WRITE_TIMEOUT` | 写超时（秒） | `300` |
| `MYSQL_AUTOCOMMIT` | 自动提交 | `false` |
| `MYSQL_MINSIZE` / `MYSQL_MAXSIZE` | 异步池大小 | `1` / `10` |
| `MYSQL_POOL_RECYCLE` | 连接回收（秒） | `1800` |

建议在运行前通过环境变量注入账号密码，勿将凭据写入代码仓库。

## 目录结构

```
common_mysql/
├── config.py          # MySQLConfig 与环境变量解析
├── sync_mysql.py      # SyncMySQLClient
├── async_mysql.py     # AsyncMySQLClient（连接池、分块写入）
├── mysql_builder.py   # SQL 构建辅助（供业务模块引用）
├── example_usage.py   # 同步/异步示例
└── __init__.py
```

## 同步示例

```python
from tools.common_mysql import MySQLConfig, SyncMySQLClient

cfg = MySQLConfig.from_env()
with SyncMySQLClient(cfg) as client:
    rows = client.query_all("SELECT 1 AS num")
    print(rows)

    with client.transaction():
        client.executemany(
            "INSERT INTO demo(name) VALUES (%s)",
            [("a",), ("b",)],
        )
```

## 异步示例

```python
import asyncio
from tools.common_mysql import AsyncMySQLClient, MySQLConfig

async def main() -> None:
    cfg = MySQLConfig.from_env()
    async with AsyncMySQLClient(cfg) as client:
        one = await client.query_one("SELECT COUNT(*) AS c FROM demo")
        await client.executemany_chunked(
            "INSERT INTO demo(name) VALUES (%s)",
            [("x",), ("y",)],
            chunk_size=500,
            retries=3,
        )

asyncio.run(main())
```

大批量写入可使用 `executemany_chunked_parallel`（按块并发，需合理设置 `max_concurrency`）。

## 主要 API

| 类 / 方法 | 说明 |
|-----------|------|
| `MySQLConfig.from_env(prefix="MYSQL_")` | 从环境变量构建配置 |
| `SyncMySQLClient` | `connect` / `close` / `execute` / `query_one` / `query_all` / `executemany` / `transaction` |
| `AsyncMySQLClient` | 同上（异步）+ `executemany_chunked` / `executemany_chunked_parallel` |

游标默认返回字典行（`DictCursor`），可通过 `use_dict_cursor=False` 关闭。

## 本地验证

```bash
# 设置 MYSQL_* 后
python tools/common_mysql/example_usage.py
```

## 相关模块

- `tools/component_tag_classify`：批处理写回 MySQL
- `tools/common_hive`：Hive 侧接口风格与之对齐
