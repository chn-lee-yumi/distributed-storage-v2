import hashlib
import os

import requests

SERVER = "10.202.5.51:8080"
TEST_FILE = "~/Downloads/test.wav"
TEST_FILE2 = "~/Downloads/test.wav.bk"
print(os.name)


def calc_sha1(filepath):
    print(f"计算SHA1：{filepath}")
    sha1obj = hashlib.sha1()
    with open(filepath, 'rb') as f:
        data = f.read()
    sha1obj.update(data)
    print(sha1obj.hexdigest())
    return sha1obj.hexdigest()


def upload(path):
    print("【上传文件】")
    file = {'file': open(path, 'rb')}
    res = requests.post(url=f"http://{SERVER}/api/file", files=file)
    print(res.status_code, res.text)
    return res.text


def download(file_id):
    print("【下载文件】")
    res = requests.get(url=f"http://{SERVER}/api/file/{file_id}")
    with open(f'./tmp.file.{file_id}', 'wb') as f:
        f.write(res.content)


def delete(file_id):
    print("【删除文件】")
    res = requests.delete(url=f"http://{SERVER}/api/file/{file_id}")
    print(res.status_code, res.text)


# 测试
# req = requests.get(url=f"http://{SERVER}/api/node/")
# print(req.status_code, req.text)
file_id = upload(TEST_FILE)
# file_id2 = upload(TEST_FILE2)
# file_id = "4c39c112-5b49-11ec-ab4d-525400a7a71e"
# file_id2 = "6d21a4fc-5b45-11ec-98bd-525400a7a71e"
download(file_id)
# download(file_id2)
calc_sha1(TEST_FILE)
# calc_sha1(TEST_FILE2)
calc_sha1(f'./tmp.file.{file_id}')
# calc_sha1(f'./tmp.file.{file_id2}')
# delete(file_id)
# delete(file_id2)
os.remove(f'./tmp.file.{file_id}')
# os.remove(f'./tmp.file.{file_id2}')
