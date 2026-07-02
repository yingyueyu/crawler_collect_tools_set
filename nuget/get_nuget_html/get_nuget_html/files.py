from datetime import timedelta
import io
import os

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error
from minio.deleteobjects import DeleteObject

from .utils.logs import get_default_logger

logger = get_default_logger(name="minio_files", log_dir="app_logs", max_file_mb=50)

from tools.key_token_config import (
    MINIO_12,
    MINIO_168,
    MINIO_61_TEST,
    MINIO_99,
    MINIO_DEFAULT,
    MINIO_LOCAL_TEST,
)

configs_test = MINIO_LOCAL_TEST
configs = MINIO_DEFAULT
configs_99 = MINIO_99
configs_168 = MINIO_168
configs_12 = MINIO_12


def save_file(file_str, file_path, file_name):
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    with open(os.path.join(file_path, file_name), 'w') as f:
        f.write(file_str)


class MinIOClient(object):
    client = None
    policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::%s"]},{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}'

    # def __new__(cls, *args, **kwargs):
    #     if not cls.client:
    #         cls.client = object.__new__(cls)
    #     return cls.client
    
    def __init__(self, service, access_key, secret_key, secure=False):
        self.service = service
        self.client = Minio(service, access_key=access_key, secret_key=secret_key, secure=secure)

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
            return response.data.decode()
        except S3Error as ex:
            pass
    
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

minio_client_test = MinIOClient(service=configs_test['url'], access_key=configs_test['accessKey'], secret_key=configs_test['secretKey'], secure=False)
minio_client = MinIOClient(service=configs['url'], access_key=configs['accessKey'], secret_key=configs['secretKey'], secure=False)
minio_client_99 = MinIOClient(service=configs_99['url'], access_key=configs_99['accessKey'], secret_key=configs_99['secretKey'], secure=False)
minio_client_168 = MinIOClient(service=configs_168['url'], access_key=configs_168['accessKey'], secret_key=configs_168['secretKey'], secure=False)
minio_client_12 = MinIOClient(service=configs_12['url'], access_key=configs_12['accessKey'], secret_key=configs_12['secretKey'], secure=False)

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



if __name__=='__main__':
    # import requests
    # response = requests.get('https://min.io/docs/minio/linux/index.html')
    # minio_client.put_file('data', 'index.html', response.text)
    # print(minio_client.get_file('data', 'index.html'))
    # nuget_deal()
    html = minio_client_168.get_file('github', 'tairesh/necromanzer/detail.html')
    print(html)