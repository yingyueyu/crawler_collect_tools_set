import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from random import random
from urllib.parse import urlparse
import requests
from typing import Union, List, Tuple

from .logs import get_default_logger


class GitHubTagsCounter:
    def __init__(self, tokens: Union[str, List[str]], thread_num: int = 10, error_path: str = None):
        """
        初始化GitHub标签计数器

        Args:
            tokens: GitHub访问令牌，可以是单个字符串或列表
            thread_num: 线程池大小，默认为10
            error_path: 错误URL保存路径，默认为None
        """
        print("GitHub标签计数器初始化中...", tokens)
        self.tokens = tokens if isinstance(tokens, list) else [tokens]
        self.thread_num = thread_num
        self.error_path = error_path or f"errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self.logger = get_default_logger(
            name="github_tags_counter",
            log_dir="app_logs",
            max_file_mb=50,
        )

    def extract_repo_info(self, repo_url: str) -> Tuple[str, str]:
        """解析GitHub仓库URL，提取owner和repo名称"""
        try:
            if not repo_url.startswith('http') and repo_url.startswith('pkg:github/'):
                repo_url = repo_url.replace('pkg:github/', 'https://github.com/')

            parsed_url = urlparse(repo_url)
            path = parsed_url.path.strip('/')
            parts = path.split('/')

            if len(parts) < 2:
                raise ValueError(f"Invalid GitHub repository URL: {repo_url}")

            return parts[0], parts[1]
        except Exception as e:
            self.logger.error(f"Failed to parse URL {repo_url}: {str(e)}")
            raise

    def get_tags_count(self, owner: str, repo: str) -> int:
        """使用GraphQL API获取仓库的标签数量"""
        url = "https://api.github.com/graphql"
        query = """
        query($owner: String!, $repo: String!) {
          repository(owner: $owner, name: $repo) {
            refs(refPrefix: "refs/tags/", first: 0) {
              totalCount
            }
          }
        }
        """

        variables = {"owner": owner, "repo": repo}

        # 轮询使用不同的token
        for token in self.tokens:
            try:
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }

                response = requests.post(
                    url,
                    json={"query": query, "variables": variables},
                    headers=headers,
                    timeout=10
                )

                if response.status_code == 401:  # Token无效，尝试下一个
                    continue

                if response.status_code != 200:
                    raise Exception(f"Query failed with status {response.status_code}: {response.text}")

                data = response.json()
                if "errors" in data:
                    raise Exception(f"GraphQL errors: {data['errors']}")

                return data["data"]["repository"]["refs"]["totalCount"]

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed for {owner}/{repo}: {str(e)}")
                continue

        raise Exception("All tokens failed or rate limited")

    def process_single_repo(self, repo_url: str) -> Tuple[str, Union[int, str]]:
        """处理单个仓库URL"""
        try:
            owner, repo = self.extract_repo_info(repo_url)
            tags_count = self.get_tags_count(owner, repo)
            self.logger.info(f"Successfully processed {repo_url}: {tags_count} tags")
            return repo_url, tags_count
        except Exception as e:
            self.logger.error(f"Failed to process {repo_url}: {str(e)}")
            # 保存错误URL
            with open(self.error_path, 'a', encoding='utf-8') as f:
                f.write(f"{repo_url}\n")
            return repo_url, f"Error: {str(e)}"

    def process_repos_from_csv(self, csv_path: str, save_path: str) -> None:
        """从CSV文件读取仓库URL并处理"""
        # 读取所有URL
        with open(csv_path, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]

        # 清空错误文件
        if os.path.exists(self.error_path):
            os.remove(self.error_path)

        results = []

        # 使用线程池处理
        with ThreadPoolExecutor(max_workers=self.thread_num) as executor:
            # 提交所有任务
            future_to_url = {executor.submit(self.process_single_repo, url): url for url in urls}

            # 处理完成的任务
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"Unexpected error processing {url}: {str(e)}")
                    results.append((url, f"Unexpected error: {str(e)}"))

        # 保存结果
        with open(save_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['URL', 'Tags Count'])
            writer.writerows(results)

        self.logger.info(f"Processing completed. Results saved to {save_path}")
        if os.path.exists(self.error_path):
            self.logger.info(f"Failed URLs saved to {self.error_path}")


def git_github_repo_tags(csv_path: str, save_path: str, tokens: Union[str, List[str]], thread_num: int = 10):
    """
    主函数：处理GitHub仓库标签统计

    Args:
        csv_path: 输入CSV文件路径
        save_path: 结果保存路径
        tokens: GitHub访问令牌
        thread_num: 线程数，默认为10
    """
    counter = GitHubTagsCounter(tokens=tokens, thread_num=thread_num)
    counter.process_repos_from_csv(csv_path, save_path)


if __name__ == '__main__':
    import random

    from tools.key_token_config import GITHUB_TOKENS

    tokens = GITHUB_TOKENS
    # # 使用单个token
    # git_github_repo_tags(
    #     csv_path=csv_path,
    #     save_path=save_path,
    #     tokens=tokens,
    #     thread_num=10
    # )
    counter = GitHubTagsCounter(tokens=random.choice(tokens))
    url, tags_count =counter.process_single_repo("https://github.com/facebook/react/")
    print(url, tags_count)
    # 使用多个token
    # git_github_repo_tags(
    #     csv_path='input.csv',
    #     save_path='output.csv',
    #     tokens=['token1', 'token2', 'token3'],
    #     thread_num=20
    # )

    # 689195