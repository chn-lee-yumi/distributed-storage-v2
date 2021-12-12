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
    file = {'file': open("~/Downloads/test.wav", 'rb')}
    res = requests.post(url="http://127.0.0.1:8080/api/file", files=file)
    print(res.status_code, res.text)


def download():
    print("【下载文件】")
    res = requests.get(url="http://127.0.0.1:8080/api/file/65d9739d1ab12a398854465d37fb6279")
    with open('./tmp.file', 'wb') as f:
        f.write(res.content)


def delete():
    print("【删除文件】")
    res = requests.delete(url="http://127.0.0.1:8080/api/file/65d9739d1ab12a398854465d37fb6279")
    print(res.status_code, res.text)


# 测试
req = requests.get(url="http://127.0.0.1:8080/api/node/127.0.0.1/8002/10")
print(req.status_code, req.text)
upload()
download()
calc_sha1("~/Downloads/test.wav")
calc_sha1("./tmp.file")
delete()
delete()
os.remove("./tmp.file")
