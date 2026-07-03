import pandas as pd
import redis
import time
import os
import sys
from pathlib import Path
from tqdm import tqdm
import pickle

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.key_token_config import REDIS_GIT_GET_HTML

# Redis配置
REDIS_HOST = REDIS_GIT_GET_HTML["host"]
REDIS_PORT = REDIS_GIT_GET_HTML["port"]
REDIS_DB = REDIS_GIT_GET_HTML["db"]
# REDIS_HOST = '127.0.0.1'
# REDIS_PORT = 6379
# REDIS_DB = 0
# Scrapy-Redis使用的键名
# SCHEDULER_QUEUE_KEY = 'github_html:urls'  # 默认请求队列键
# CHECKPOINT_KEY = 'github_html:checkpoint'  # 断点续传键

# SCHEDULER_QUEUE_KEY = 'maven_html:urls'  # 默认请求队列键
# CHECKPOINT_KEY = 'maven_html:checkpoint'  # 断点续传键

# SCHEDULER_QUEUE_KEY = 'go_html:urls'  # 默认请求队列键
# CHECKPOINT_KEY = 'go_html:checkpoint'  # 断点续传键


SCHEDULER_QUEUE_KEY = 'pypi_html:urls'  # 默认请求队列键
CHECKPOINT_KEY = 'pypi_html:checkpoint'  # 断点续传键


# 导入配置
CSV_PATH = 'test_code/test_purl_list.csv'
BATCH_SIZE = 200  # 每批处理数量
WAIT_INTERVAL = 5  # 检查间隔（秒）
MAX_WAIT_TIME = 300  # 最大等待时间（秒）
HAS_HEADER = False  # CSV是否有标题行


def push_to_scrapy_redis(r, urls):
    """将URL批量推送到Scrapy-Redis队列"""
    with r.pipeline() as pipe:
        for url in urls:
            # 创建Scrapy请求对象
            # request_data = {
            #     'url': url,
            #     'callback': 'parse',  # 替换为您的回调方法
            #     'meta': {'source': 'csv_importer'},
            #     'dont_filter': True  # 根据需求设置
            # }
            # 序列化请求（实际项目中可能需要使用Scrapy的序列化方法）
            # serialized = pickle.dumps(request_data)
            pipe.lpush(SCHEDULER_QUEUE_KEY, url)
        pipe.execute()


def wait_for_queue_empty(r):
    """等待Scrapy-Redis队列变空"""
    start_time = time.time()

    while True:
        queue_size = r.llen(SCHEDULER_QUEUE_KEY)

        if queue_size == 0:
            print("队列已空，可以推送下一批数据")
            return True

        if time.time() - start_time > MAX_WAIT_TIME:
            print(f"等待超时({MAX_WAIT_TIME}秒)，队列仍有 {queue_size} 个项目")
            return False

        print(f"队列中还有 {queue_size} 个项目，等待中...")
        time.sleep(WAIT_INTERVAL)


def get_last_position(r):
    """获取上次处理的位置"""
    position = r.get(CHECKPOINT_KEY)
    return int(position.decode()) if position else 0


def save_checkpoint(r, position):
    """保存当前处理位置"""
    r.set(CHECKPOINT_KEY, position)


def calculate_total_rows():
    """计算CSV文件总行数"""
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)


def main():
    # 连接Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

    try:
        # 测试Redis连接
        r.ping()
        print("成功连接到Redis")
    except redis.ConnectionError:
        print("无法连接到Redis")
        return

    # 获取上次处理的位置
    last_position = get_last_position(r)
    total_rows = calculate_total_rows()

    # 计算实际数据行数（减去标题行）
    data_rows = total_rows - 1 if HAS_HEADER else total_rows

    if last_position > 0:
        print(f"检测到上次处理位置: 行 {last_position}/{data_rows} ({last_position / data_rows:.1%})")
        print("从断点继续导入...")
    else:
        print("开始新导入...")

    try:
        # 使用pandas分块读取
        processed_count = 0
        skipped_rows = 0

        # 创建进度条（从上次位置继续）
        with tqdm(total=data_rows, initial=last_position, desc="导入进度") as pbar:
            # 计算需要跳过的行数
            skip_rows = last_position
            if HAS_HEADER:
                skip_rows += 1  # 跳过标题行

            # 分块读取CSV
            for chunk in pd.read_csv(CSV_PATH, chunksize=BATCH_SIZE,
                                     skiprows=range(1, skip_rows) if skip_rows > 0 else None):

                # 提取第一列URL
                urls = chunk.iloc[:, 0].astype(str).tolist()
                clean_urls = [url.strip() for url in urls if url.strip()]

                current_batch_size = len(urls)
                processed_count += len(clean_urls)

                # 如果没有有效URL，跳过这批
                if not clean_urls:
                    skipped_rows += current_batch_size
                    # 更新位置
                    last_position += current_batch_size
                    save_checkpoint(r, last_position)
                    pbar.update(current_batch_size)
                    continue

                # 推送到Scrapy-Redis队列
                print(f"推送 {len(clean_urls)} 个URL到{SCHEDULER_QUEUE_KEY}")
                push_to_scrapy_redis(r, clean_urls)

                # 更新位置
                last_position += current_batch_size
                save_checkpoint(r, last_position)
                pbar.update(current_batch_size)

                # 等待队列处理完毕
                if not wait_for_queue_empty(r):
                    print("超时，继续处理下一批...")

                # 添加短暂延迟防止过载
                time.sleep(0.1)

        print(f"\n导入完成! 共推送 {processed_count} 个URL到{SCHEDULER_QUEUE_KEY}")
        if skipped_rows > 0:
            print(f"跳过 {skipped_rows} 个空行或无效URL")

        # 导入完成后清除检查点
        r.delete(CHECKPOINT_KEY)
        print("检查点已清除，导入任务完成")

    except KeyboardInterrupt:
        print("\n用户中断! 已保存当前进度。")
        print(f"下次将从行 {last_position} 继续")
    except Exception as e:
        print(f"处理失败: {str(e)}")
        print(f"错误发生位置: 行 {last_position}")
    finally:
        r.close()


if __name__ == "__main__":
    main()