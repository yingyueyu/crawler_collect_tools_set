from cgi import test
from datetime import timedelta
import io
import os
import platform
import urllib.parse
import urllib3
from loguru import logger
from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error
from minio.deleteobjects import DeleteObject


from tools.key_token_config import (
    MINIO_168,
    MINIO_61_TEST,
    MINIO_99,
    MINIO_DEFAULT,
)

configs = MINIO_168
configs_99 = MINIO_99
configs_test = MINIO_61_TEST

class MinIOClient(object):
    client = None
    policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::%s"]},{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}'

    # def __new__(cls, *args, **kwargs):
    #     if not cls.client:
    #         cls.client = object.__new__(cls)
    #     return cls.client
    
    def __init__(
        self,
        service,
        access_key,
        secret_key,
        secure=False,
        pool_maxsize: int = 64,
        timeout: int = 30,
    ):
        """
        pool_maxsize: 到同一 MinIO 主机的 urllib3 连接池大小。批量多线程拉对象时默认仅约 10 条连接，
        线程数开很大也会在池外排队，吞吐上不去、CPU 也很低（都在等网络）。可按并发线程数调高（如 32～128）。
        timeout: 单次请求连接/读取超时（秒），避免 get_object 永久阻塞。
        """
        self.service = service
        http_client = urllib3.PoolManager(
            maxsize=pool_maxsize,
            timeout=urllib3.Timeout(connect=timeout, read=timeout),
        )
        self.client = Minio(
            service,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            http_client=http_client,
        )

    def remove_file(self, bucket_name, file):
        """
        移除单个文件
        :return:
        """
        self.client.remove_object(bucket_name, file)

    def get_file(self, bucket_name, file):
        """
        从bucket 下载文件
        :param bucket_name: 桶名
        :param file: 文件名
        :return:
        """
        try:
            response = self.client.get_object(bucket_name, file)
            try:
                return response.read().decode("utf-8", errors="replace")
            finally:
                response.close()
                response.release_conn()
        except S3Error:
            return None
        except Exception:
            return None
    
    def get_bytes(self, bucket_name, file):
        """
        从bucket 下载文件
        :param bucket_name: 桶名
        :param file: 文件名
        :return:
        """
        try:
            response = self.client.get_object(bucket_name, file)
            return response.data
        except S3Error as ex:
            pass
    def fget_file(self, bucket_name, file, file_path):
        """
        下载保存文件保存本地
        :param bucket_name: 桶名
        :param file: 文件名
        :param file_path: 本地文件路径
        :return:
        """
        try:
            self.client.fget_object(bucket_name, file, file_path)
        except S3Error as ex:
            pass

    def put_file(self, bucket_name, file, file_data):
        """
        上传文件 + 写入
        :param bucket_name: 桶名
        :param file: 文件名
        :param file_data: 本地文件路径
        :return:
        """
        data_size = len(file_data.encode())
        self.client.put_object(bucket_name, file, io.BytesIO(file_data.encode()), data_size)
        # local_file = os.path.join(bucket_name, file)
        # file_path = '/'.join(local_file.split('/')[:-1])
        # if not os.path.exists(file_path):
        #     os.makedirs(file_path)
        # with open(local_file, 'w') as f:
        #     f.write(file_data)


    def put_bytes(self, bucket_name, file, bytes_data):
        """
        上传文件 + 写入
        :param bucket_name: 桶名
        :param file: 文件名
        :param bytes_data: 文件
        :return:
        """
        data_size = len(bytes_data)
        self.client.put_object(bucket_name, file, io.BytesIO(bytes_data), data_size)

    def fput_file(self, bucket_name, file, file_path):
        """
        上传文件
        :param bucket_name: 桶名
        :param file: 文件名
        :param file_path: 本地文件路径
        :return:
        """
        self.client.fput_object(bucket_name, file, file_path)
        
    
    def copy_file(self, bucket_name, file, file_path):
        """
        拷贝文件（最大支持5GB）
        :param bucket_name:
        :param file:
        :param file_path:
        :return:
        """
        self.client.copy_object(bucket_name, file, CopySource(bucket_name, file_path))        


minio_client = MinIOClient(service=configs['url'], access_key=configs['accessKey'], secret_key=configs['secretKey'], secure=False)

minio_client_99 = MinIOClient(service=configs_99['url'], access_key=configs_99['accessKey'], secret_key=configs_99['secretKey'], secure=False)

minio_client_168 = MinIOClient(service=configs_168['url'], access_key=configs_168['accessKey'], secret_key=configs_168['secretKey'], secure=False)

minio_client_test = MinIOClient(service=configs_test['url'], access_key=configs_test['accessKey'], secret_key=configs_test['secretKey'], secure=False)



def nuget_deal():
    bucket_name = 'nuget'
    with open('/home/documents/nuget/nuget.csv') as f:
        line = f.readline()
        count = 0
        while line:
            print(count, line)
            try:
                if count < 340582:
                    continue
                module, version = line.split(',')[:2]
                last = minio_client.get_file(bucket_name, f"{module}/last.html")
            except S3Error as ex:
                if ex.code == 'NoSuchKey':
                    try:
                        minio_client.copy_file(bucket_name, f"{module}/last.html", f"{module}/{version}.html")
                    except:
                        logger.exception(ex)
                else:
                    logger.exception(ex)
            finally:
                line = f.readline()
                count += 1


# minio client url路径转化
def minio_client_url_to_path(platform_source: str, bucket_name: str, url: str, html_model: str = 'detail') -> str:
    """
    将minio client url路径根据桶名所对应规则转化为minio本地文件路径
    :param bucket_name: 桶名
    :param url: url路径
    :return: minio本地文件路径
    """
    url = url.strip('\n').strip('/')
    minio_path = None
    if platform_source == 'skills':
        if url.startswith('https://skills.sh/'):
            minio_path = f"{url.replace('https://skills.sh/', '')}/detail.html"
        elif url.startswith('pkg:skills/'):
            minio_path = f"{url.replace('pkg:skills/', '')}/detail.html"
        else:
            minio_path = url
    elif platform_source == 'github':
        if url.startswith('https://github.com/'):
            minio_path = f"{url.replace('https://github.com/', '')}/detail.html"
        elif url.startswith('pkg:github/'):
            minio_path = f"{url.replace('pkg:github/', '')}/detail.html"
        else:
            minio_path = url
    elif platform_source == 'maven':
        if html_model == 'detail':
            groupId, artifactId = urllib.parse.urlparse(url).path.split('/')[-2:]
            minio_path = f"{groupId}/{artifactId}/versions.html"

    elif platform_source == 'protectai':
        if html_model == 'detail':
            # 转化格式
            if url.startswith('https://protectai.com/'):
                url = url.replace('https://protectai.com', '')
            if not url.startswith('/'):
                url = f'/{url}'
            if url.endswith('/overview'):
                url = f'{url}/detail.html'
            minio_path = url

    elif platform_source == 'huggingface':
        if html_model == 'detail':
            if url.startswith('https://huggingface.co/'):
                url = url.replace('https://huggingface.co/', '')
            if not url.endswith('/main'):
                url = f'{url}/main'
            if not url.endswith('detail.html'):
                url = f'{url}/detail.html'
            minio_path = url
    elif platform_source == 'pypi':
        if html_model == 'detail':
            url = url.strip('\n').strip('/')
            if url.startswith('https://pypi.org/project/'):
                url = url.replace('https://pypi.org/project/', '')
            if not url.endswith('/last.html'):
                url = url.strip('/')
                url = f'{url}/last.html'
            minio_path = url
    elif platform_source == 'npm':
        if html_model == 'detail':
            if url.startswith('https://www.npmjs.com/package/'):
                url = url.replace('https://www.npmjs.com/package/', '')
            if not url.endswith('/last.html'):
                url = f'{url}/last.html'
            minio_path = url
    elif platform_source == 'gitee':
        if html_model == 'detail':
            if url.startswith('https://gitee.com/'):
                url = f"{url.replace('https://gitee.com/', '')}/detail.html"
            elif url.startswith('pkg:gitee/'):
                url = f"{url.replace('pkg:gitee/', '')}/detail.html"
            minio_path = url
    elif platform_source == 'gitlab':
        if html_model == 'detail':
            if url.startswith('https://gitlab.'):
                url = f"{'/'.join(url.strip('/').split('/')[3:])}/detail.html"
            elif url.startswith('pkg:gitlab/'):
                url = f"{url.replace('pkg:gitlab/', '')}/detail.html"
            minio_path = url
    elif platform_source == 'atomgit':
        if html_model == 'detail':
            if url.startswith('https://atomgit.com/'):
                url = f"{url.replace('https://atomgit.com/', '')}/detail.html"
            elif url.startswith('pkg:atomgit/'):
                url = f"{url.replace('pkg:atomgit/', '')}/detail.html"
            minio_path = url
    elif platform_source == 'golang':
        if html_model == 'detail':
            if url.startswith('https://pkg.go.dev/'):
                url = f"{url.replace('https://pkg.go.dev/', '')}/last.html"
            elif url.startswith('pkg:golang/'):
                url = f"{url.replace('pkg:golang/', '')}/last.html"
            minio_path = url
    return minio_path
    





if __name__=='__main__':
    # import requests
    # response = requests.get('https://min.io/docs/minio/linux/index.html')
    # minio_client.put_file('data', 'index.html', response.text)
    # print(minio_client.get_file('data', 'index.html'))
    # nuget_deal()
    # html = minio_client_168.get_file('github', 'tairesh/necromanzer/detail.html')
    # test_html = minio_client_test.get_file('github-new', 'csstools/sanitize.css/detail.html')
    # test_html = minio_client_test.get_file('skills', 'api/git/qastudio-api/detail.html')
    # test_html = minio_client_test.get_file('protectai', '/insights/models/all-oj-gen/all_oj_pair4_ds_coder6.7b_reflct_rmsprop_iter3/4a387785fd2c98f5f70dfec7efc6b618f2af42e4/overview/detail.html')
    # test_html = minio_client_test.get_file('protectai', '/insights/models/all-oj-gen/all_oj_pair4_ds_coder6.7b_reflct_rmsprop_iter3/4a387785fd2c98f5f70dfec7efc6b618f2af42e4/overview/detail.html')
    # test_html = minio_client_test.get_file('huggingface-protectai', 'ChangeIsKey/graded-wsd/main/detail.html')
    platform_source = 'github-new'
    # file_name = 'africa.absa/inception-config/versions.html'
    url_name = 'numpy/numpy'
    file_name = f'{url_name}/detail.html'
    # file_name = f'{url_name}/last.html'# file_name = f'{url_name}/detail.html'
    test_html = minio_client_test.get_file(platform_source, file_name)
    print(test_html)
    # 测试解析
    from lxml import html
    import json
    # tree = html.fromstring(test_html)
    # links = tree.xpath('//div[@class="UnitMeta-repo"]/a/@href')[0]
    #     # 去重
    # # links = list(set(links))
    # print(links)
    
    # npm_url = tree.xpath('//meta[@name="twitter:url"]/@content')
    #     # 提取npm_url中的包名：package/包名
    # if npm_url:
    #     package_name = npm_url[0].strip('/').split('package/')[-1]
    #     print(package_name)

    # # npm 源代码托管地
    # repository_url = tree.xpath('//a[@aria-labelledby="repository repository-link"]/@href')
    # if repository_url:
    #     repository_url = repository_url[0]
    #     print(repository_url)


    # # npm 组件官网
    # homepage_url = tree.xpath('//a[@aria-labelledby="homePage homePage-link"]/@href')
    # if homepage_url:
    #     homepage_url = homepage_url[0]
    #     print(homepage_url)

    # maven 
    




    # repo_id = tree.xpath('//meta[@name="octolytics-dimension-repository_id"]/@content')
    #     # repo_name 
    # repo_name = tree.xpath("//meta[@property='og:url']/@content")
    # print(repo_id, repo_name)
    # print(html_doc.xpath('//div[@class="flex flex-row items-center gap-2"]//text()'))
    # # print(html_doc.xpath('//a[@data-testid="version-card"][3]//text()'))
    # data_props = html_doc.xpath('//div[@class="SVELTE_HYDRATER contents" and @data-target="ViewerIndexTreeList"]/@data-props')[0]
    # print(data_props)
    # json_data = json.loads(data_props)
    # print(json_data)
    # print(html_doc.xpath('//div[@class="SVELTE_HYDRATER contents" and @data-target="ViewerIndexTreeList"]/@data-props')[0])
    # test_file = "D:\\DBeaver_export_2\\all_oj_pair4_ds_coder6.7b_reflct_rmsprop_iter3.html"
    # test_file = "D:\\DBeaver_export_2\\popV.html"
    # with open(test_file, 'r', encoding='utf-8') as f:
    #     test_html = f.read()
    # html_doc = html.fromstring(test_html)
    # print(len(html_doc.xpath('//a[@data-testid="version-card"]')))

    # 上传文件到minio
    # file_path= "D:\\DBeaver_export_2\\popV.html"
    # save_name = "/insights/models/popV/tabula_muris_Pancreas_10x/3d47a30b1db3bd219d7a3b6eb9de2be6b92d53ba/overview/detail.html"
    # minio_client_test.fput_file('protectai', save_name, file_path)