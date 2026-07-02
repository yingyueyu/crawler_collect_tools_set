from datetime import timedelta
import io
import os
import sys

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error
from minio.deleteobjects import DeleteObject
import json
sys.path.append('..')

# from confs.minio_conf import MINIO_CONFIG, MINIO_CONFIG_168


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

    def exists_bucket(self, bucket_name):
        """
        判断桶是否存在
        :param bucket_name: 桶名称
        :return:
        """
        return self.client.bucket_exists(bucket_name=bucket_name)
    
    def create_bucket(self, bucket_name:str, is_policy:bool=True):
        """
        创建桶 + 赋予策略
        :param bucket_name: 桶名
        :param is_policy: 策略
        :return:
        """
        if self.exists_bucket(bucket_name=bucket_name):
            return False
        else:
            self.client.make_bucket(bucket_name = bucket_name)
        if is_policy:
            policy = self.policy % (bucket_name, bucket_name)
            self.client.set_bucket_policy(bucket_name=bucket_name, policy=policy)
        return True

    def get_bucket_list(self):
        """
        列出存储桶
        :return:
        """
        buckets = self.client.list_buckets()
        bucket_list = []
        for bucket in buckets:
            bucket_list.append(
                {"bucket_name": bucket.name, "create_time": bucket.creation_date}
            )
        return bucket_list

    def remove_bucket(self, bucket_name):
        """
        删除桶
        :param bucket_name:
        :return:
        """
        try:
            self.client.remove_bucket(bucket_name=bucket_name)
        except S3Error as e:
            print("[error]:", e)
            return False
        return True
    
    def list_files(self, bucket_name, prefix=None):
        """
        列出存储桶中所有对象
        :param bucket_name: 同名
        :param prefix: 前缀
        :return:
        """
        try:
            files = []
            files_list = self.client.list_objects(bucket_name=bucket_name, prefix=prefix, recursive=True)
            print(111,)
            for obj in files_list:
                print(obj.bucket_name, obj.object_name, obj.last_modified,
                      obj.etag, obj.size, obj.content_type)
                files.append(obj.object_name)
            print(len( files))
            return files
        except S3Error as e:
            print("[error]:", e)

    def bucket_policy(self, bucket_name):
        """
        列出桶存储策略
        :param bucket_name:
        :return:
        """
        try:
            policy = self.client.get_bucket_policy(bucket_name)
        except S3Error as e:
            print("[error]:", e)
            return None
        return policy

    def download_file(self, bucket_name, file, file_path, stream=1024*32):
        """
        从bucket 下载文件 + 写入指定文件
        :return:
        """
        data = self.client.get_object(bucket_name, file)
        with open(file_path, "wb") as fp:
            for d in data.stream(stream):
                fp.write(d)

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

    def fget_file(self, bucket_name, file, file_path):
        """
        下载保存文件保存本地
        :param bucket_name:
        :param file:
        :param file_path:
        :return:
        """
        try:
            self.client.fget_object(bucket_name, file, file_path)
        except S3Error as ex:
            pass

    def copy_file(self, bucket_name, file, file_path):
        """
        拷贝文件（最大支持5GB）
        :param bucket_name:
        :param file:
        :param file_path:
        :return:
        """
        try:
            self.client.copy_object(bucket_name, file, CopySource(bucket_name, file_path))
        except S3Error as ex:
            pass

    def upload_file(self, bucket_name, file, file_path, content_type):
        """
        上传文件 + 写入
        :param bucket_name: 桶名
        :param file: 文件名
        :param file_path: 本地文件路径
        :param content_type: 文件类型
        :return:
        """
        try:
            with open(file_path, "rb") as file_data:
                file_stat = os.stat(file_path)
                self.client.put_object(bucket_name, file, file_data, file_stat.st_size, content_type=content_type)
        except S3Error as e:
            print("[error]:", e)

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

    def put_file(self, bucket_name, file, file_data):
        """
        上传文件 + 写入
        :param bucket_name: 桶名
        :param file: 文件名
        :param file_path: 本地文件路径
        :param content_type: 文件类型
        :return:
        """

        data_size = len(file_data.encode())
        self.client.put_object(bucket_name, file, io.BytesIO(file_data.encode()), data_size)
            
    def fput_file(self, bucket_name, file, file_path):
        """
        上传文件
        :param bucket_name: 桶名
        :param file: 文件名
        :param file_path: 本地文件路径
        :return:
        """
        
        if not self.exists_bucket(bucket_name=bucket_name):
            self.create_bucket(bucket_name=bucket_name)
        self.client.fput_object(bucket_name, file, file_path)

    def stat_object(self, bucket_name, file):
        """
        获取文件元数据
        :param bucket_name:
        :param file:
        :return:
        """
        try:
            data = self.client.stat_object(bucket_name, file)
            print(data.bucket_name)
            print(data.object_name)
            print(data.last_modified)
            print(data.etag)
            print(data.size)
            print(data.metadata)
            print(data.content_type)
        except S3Error as e:
            print("[error]:", e)

    def remove_file(self, bucket_name, file):
        """
        移除单个文件
        :return:
        """
        self.client.remove_object(bucket_name, file)

    def remove_files(self, bucket_name, file_list):
        """
        删除多个文件
        :return:
        """
        delete_object_list = [DeleteObject(file) for file in file_list]
        for del_err in self.client.remove_objects(bucket_name, delete_object_list):
            print("del_err", del_err)

    def presigned_get_file(self, bucket_name, file, days=7):
        """
        生成一个http GET操作 签证URL
        :return:
        """
        return self.client.presigned_get_object(bucket_name, file, expires=timedelta(days=days))

from tools.key_token_config import MINIO_61_TEST

configs_test = MINIO_61_TEST
# minio_client = MinIOClient(service=MINIO_CONFIG['service'], access_key=MINIO_CONFIG['accessKey'], secret_key=MINIO_CONFIG['secretKey'], secure=False)
minio_client_61 = MinIOClient(service=configs_test['url'], access_key=configs_test['accessKey'], secret_key=configs_test['secretKey'], secure=False)


# minio_client_168 = MinIOClient(service=MINIO_CONFIG_168['url'], access_key=MINIO_CONFIG_168['accessKey'], secret_key=MINIO_CONFIG_168['secretKey'], secure=False)


if __name__=='__main__':
    # root_path = '/home/datas/python/pypi/'
    # bucket_name = 'gitlab'
    minio_client_61.create_bucket(bucket_name='golang-2026')
    # for root, dirs, files in os.walk(root_path):
    #     for file in files:
    #         if file.endswith('.html'):
    #             minio_file = os.path.join(root, file).replace(root_path, '')
    #             local_file = os.path.join(root, file)
    #             minio_client.fput_file(bucket_name, minio_file, local_file)
    # test = MinIOClient(service='minio.example.com:19000', access_key='your-access-key', secret_key='your-secret-key', secure=False)
    # res = test.get_file('test', 'test.html')
    # print(res)
    # import csv
    # files = minio_client.list_files('gitlab')
    # with open('output1.csv', 'w', newline='', encoding='utf-8') as f:
    #     writer = csv.writer(f)
    #     for item in files:
    #         writer.writerow([item.replace('/detail.html','')])
    # minio_client.remove_files(bucket_name='atomgit', file_list=files)
    # minio_client.remove_file('gitee', f'openharmony-sig/device_soc_winnermicro/detail.html')

    # for file in files:
    #     print(file)
    #     minio_client_168.copy_file('fsf', f'{file}.html', file)
    # html = minio_client.get_file('gitlab', f'gnome/libgit2-glib/detail.json')
    # print(html)
    # print(json.loads(html ))
    # from lxml.html import etree
    # html_doc = etree.HTML(html)
    # href = html_doc.xpath('//*[@title="OpenHarmony"]/text()')[0]
    # print(href)

    # minio_client.fput_file('gitee', 'openharmony-sig/neural_network_runtime/detail.html', r'E:\project_new\datas-center\test\index.html')
    # pass