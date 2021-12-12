import os
import random
import socket
import threading

import requests
import time
from flask import Flask, request, url_for, redirect

SERVER = "10.202.5.51:8080"  # 服务器地址
PORT = 8002  # 存储节点监听端口
NODE_NAME = socket.gethostname()  # 存储节点名字（暂时用主机名代替）
DISK_SPACE = 10  # 存储节点可以提供的硬盘空间大小，单位GB

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 + 256  # 上传文件大小限制：16M 这是存储节点支持的最大块大小


# TODO：启动时扫描本地块，和服务器上的对比，看有没有多余的，删掉
# TODO：心跳线程优雅退出

@app.route("/api/block/<string:block_hash>", methods=['GET'])
def download_block(block_hash: str):
    """下载块"""
    return redirect(url_for('static', filename=f'block/{block_hash}'))  # TODO: send_from_directory


@app.route("/api/block/<string:block_hash>", methods=['POST'])
def upload_block(block_hash: str):
    """上传块"""
    file = request.files['file']
    file.save(f'./static/block/{block_hash}')
    return "OK", 203


@app.route("/api/block/<string:block_hash>", methods=['DELETE'])
def delete_block(block_hash: str):
    """删除块"""
    try:
        os.remove(f'./static/block/{block_hash}')
    except FileNotFoundError:
        return "", 404
    return "", 204


@app.route("/api/sync_block/<string:node_addr>/<string:block_hash>", methods=['POST'])
def sync_block(node_addr: str, block_hash: str):
    """从另一个节点同步块"""
    res = requests.get(url=f"http://{node_addr}/api/block/{block_hash}")
    with open(f'./static/block/{block_hash}', 'wb') as f:
        f.write(res.content)
    return "synced", 204


# TODO：客户端下载文件的接口


@app.route("/api/status", methods=['GET'])
def status():
    """存储节点状态"""
    return f"{SERVER} {PORT} {NODE_NAME} {DISK_SPACE}", 200


def heartbeat():
    """每5秒发送一次心跳"""
    time.sleep(random.randint(1, 5))
    while True:
        try:
            res = requests.post(f"http://{SERVER}/api/node/{NODE_NAME}/{PORT}/{DISK_SPACE}")
            print(res.status_code, res.text)
        except Exception as e:
            print("发送心跳失败：", e)
        time.sleep(5)


if __name__ == '__main__':
    # 创建心跳线程
    heartbeat_thread = threading.Thread(target=heartbeat)
    heartbeat_thread.start()
    # 启动存储节点
    app.run(host="0.0.0.0", port=PORT)
