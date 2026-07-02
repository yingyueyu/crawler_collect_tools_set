from __future__ import annotations

import asyncio

from tools.common_mysql import AsyncMySQLClient, MySQLConfig, SyncMySQLClient


def sync_demo() -> None:
    cfg = MySQLConfig.from_env()
    with SyncMySQLClient(cfg) as client:
        client.execute(
            """
            CREATE TABLE IF NOT EXISTS demo_users (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(64) NOT NULL,
                age INT NOT NULL
            )
            """
        )

        with client.transaction():
            client.executemany(
                "INSERT INTO demo_users(name, age) VALUES (%s, %s)",
                [("alice", 20), ("bob", 22)],
            )

        one = client.query_one("SELECT id, name, age FROM demo_users WHERE name=%s LIMIT 1", ("alice",))
        all_rows = client.query_all("SELECT id, name, age FROM demo_users ORDER BY id DESC LIMIT 5")
        print("sync one:", one)
        print("sync all:", all_rows)


async def async_demo() -> None:
    cfg = MySQLConfig.from_env()
    async with AsyncMySQLClient(cfg) as client:
        await client.execute(
            """
            CREATE TABLE IF NOT EXISTS demo_users_async (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(64) NOT NULL,
                age INT NOT NULL
            )
            """
        )

        async with client.transaction() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    "INSERT INTO demo_users_async(name, age) VALUES (%s, %s)",
                    [("charlie", 25), ("david", 27)],
                )

        one = await client.query_one(
            "SELECT id, name, age FROM demo_users_async WHERE name=%s LIMIT 1",
            ("charlie",),
        )
        all_rows = await client.query_all("SELECT id, name, age FROM demo_users_async ORDER BY id DESC LIMIT 5")
        print("async one:", one)
        print("async all:", all_rows)


if __name__ == "__main__":
    # 运行前先设置环境变量，例如：
    # MYSQL_HOST=127.0.0.1
    # MYSQL_PORT=3306
    # MYSQL_USER=root
    # MYSQL_PASSWORD=your_password
    # MYSQL_DATABASE=test_db
    sync_demo()
    asyncio.run(async_demo())
