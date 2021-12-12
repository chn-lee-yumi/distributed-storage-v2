import hashlib
import os

import requests


def calc_sha1(filepath):
    print(f"计算SHA1：{filepath}")
    sha1obj = hashlib.sha1()
    with open(filepath, 'rb') as f:
        data = f.read()
    sha1obj.update(data)
    print(sha1obj.hexdigest())
    return sha1obj.hexdigest()


def upload():
    print("【上传文件】")
    file = {'file': open("./main.py", 'rb')}
    res = requests.post(url="http://127.0.0.1:8002/api/block/testfile", files=file)
    print(res.status_code, res.text)


def download():
    print("【下载文件】")
    res = requests.get(url="http://127.0.0.1:8002/api/block/testfile")
    with open('./tmp.file', 'wb') as f:
        f.write(res.content)


def delete():
    print("【删除文件】")
    res = requests.delete(url="http://127.0.0.1:8002/api/block/testfile")
    print(res.status_code, res.text)


# 测试
upload()
download()
calc_sha1("./static/block/testfile")
calc_sha1("./tmp.file")
delete()
delete()
os.remove("./tmp.file")
