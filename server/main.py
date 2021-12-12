import hashlib
import mimetypes
import os
import random
import threading
import uuid

import requests
import time
from flask import Flask, request, stream_with_context, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from werkzeug.urls import url_quote

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024 + 256  # 上传文件大小限制：16G
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///./pys3.db'  # 数据库路径
db = SQLAlchemy(app)

PORT = 8080  # 服务器监听的端口
BLOCK_SIZE = 8 * 1024 * 1024  # 块大小，8MB，1GB=128个块
DEFAULT_VIRTUAL_NODE_PER_GB = 32  # 单个存储节点每GB容量的虚拟节点数量 原：128
REPLICAS = 2  # 副本数
CLUSTER_STATUS = "stable"  # 集群状态 stable稳定状态 degraded降级状态 failed有部分块已丢失
TIMEOUT = 1  # 连接超时时间，单位秒
LAST_RESTORE_TIME = 0  # 集群上一次从降级恢复到稳定的时间


# TODO：302调度功能，用户功能，web实现，集群容量状态数据
# TODO：支持range请求（断点续传）

class File(db.Model):
    """文件"""
    id = db.Column(db.String(36), primary_key=True)  # 文件的uuid，随机产生
    checksum = db.Column(db.String(40), primary_key=True)  # 文件的SHA1
    user = db.Column(db.String(24))  # 用户名限制24字节
    filename = db.Column(db.String(128))  # 文件名限制128字节
    mimetype = db.Column(db.String(32))  # mimetype
    size = db.Column(db.Integer)  # 文件大小

    def __repr__(self):
        return f'<File {self.id} {self.checksum} {self.user} {self.filename} {self.mimetype} {self.size}>'


class Block(db.Model):
    """块"""
    id = db.Column(db.String(36), primary_key=True)  # 块的uuid，随机产生
    checksum = db.Column(db.String(40))  # 块的sha1
    file_id = db.Column(db.String(40))  # 所属文件的id
    seq = db.Column(db.Integer)  # 块在文件里的序号（从0开始）
    virtual_node_id = db.Column(db.String(21))  # 块所在虚拟存储节点id
    replica_number = db.Column(db.Integer)  # 块是第几个副本【这个数值应该和存储节点的一样】 # TODO：可能没用

    def __repr__(self):
        return f'<Block {self.id} {self.checksum} {self.file_id} {self.seq} {self.virtual_node_id} {self.replica_number}>'


class VirtualNode(db.Model):
    """虚拟存储节点"""
    id = db.Column(db.String(40), primary_key=True)  # 节点id，使用(存储节点名字+seq)进行hash得到
    seq = db.Column(db.Integer)  # 是存储节点的第几个虚拟节点（从0开始，每台机默认200个虚拟节点）
    node_id = db.Column(db.String(40))  # 存储节点的id
    is_alive = db.Column(db.Boolean)  # 是否存活
    replica_number = db.Column(db.Integer)  # 这个节点保存的是第几个副本【这个数值应该和存储节点的一样】

    def __repr__(self):
        return f'<VirtualNode {self.id} {self.seq} {self.node_id} {self.is_alive} {self.replica_number}>'


class Node(db.Model):
    """存储节点"""
    id = db.Column(db.String(64), primary_key=True)  # 存储节点名字，暂时使用主机名
    address = db.Column(db.String(21))  # 访问地址，格式：xxx.xxx.xxx.xxx:xxxxx
    is_alive = db.Column(db.Boolean)  # 是否存活
    replica_number = db.Column(db.Integer)  # 这个节点保存的是第几个副本
    disk_space = db.Column(db.Integer)  # 提供的磁盘空间大小
    offline_time = db.Column(db.Integer)  # 节点掉线时间，如果为0，说明在线

    def __repr__(self):
        return f'<Node {self.id} {self.address} {self.is_alive} {self.replica_number} {self.disk_space} {self.offline_time}>'


@app.route("/api/file/<string:file_id>/<string:file_name>", methods=['GET'])
def download_file_with_filename(file_id: str, file_name: str):
    """
    查看文件（网页内查看，作为图床/直链使用）
    :param file_id: 文件id
    :param file_name: 文件名
    :return: 文件名和数据库不对应时重定向到正确文件名的url，文件名正确时返回文件内容
    """
    file = File.query.filter_by(id=file_id).first()
    if not file:
        return "File not found", 404
    if file_name != file.filename:  # 如果file_name不正确，则跳转到正确的file_name
        return redirect(url_for("download_file_with_filename", file_id=file_id, file_name=file.filename))
    headers = {
        'Content-Disposition': 'inline',  # inline表示文件可以显示在网页内，作为图床/直链使用
        'Content-Type': file.mimetype,
        'Content-Length': file.size,
    }
    # 返回文件数据流
    return app.response_class(stream_with_context(read_remote_file_blocks(file_id)), headers=headers, status=200, direct_passthrough=True)


@app.route("/api/file/<string:file_id>", methods=['GET'])
def download_file(file_id: str):
    """
    下载文件（作为网盘使用）
    :param file_id: 文件id
    :return: 文件内容
    """
    file = File.query.filter_by(id=file_id).first()
    if not file:
        return "File not found", 404
    headers = {
        'Content-Disposition': f"attachment; filename*=UTF-8''{url_quote(file.filename.encode('utf-8'))}",  # attachment表示文件应该下载，作为网盘文件共享使用
        'Content-Type': file.mimetype,
        'Content-Length': file.size,
    }
    # 返回文件数据流
    return app.response_class(stream_with_context(read_remote_file_blocks(file_id)), headers=headers, status=200, direct_passthrough=True)


@app.route("/api/file", methods=['GET'])
def list_file():
    """
    列出所有文件
    :return: 文件列表
    """
    output = ""
    files = File.query.all()
    for file in files:
        output += str(file) + "\n"
    return output


@app.route("/api/file", methods=['POST'])
def upload_file():
    """
    上传文件
    :return: 成功返回203，失败返回500
    """
    start_time = time.time()
    tmp_time0 = start_time
    # 临时保存文件
    file = request.files['file']
    filename = file.filename  # filename = secure_filename(file.filename)
    mimetype = mimetypes.guess_type(filename)[0]  # 通过文件名判断mimetype
    file_id = str(uuid.uuid1())
    tmp_file_path = f"/tmp/pys3.{file_id}.tmp"
    file.save(tmp_file_path)
    file_size = os.stat(tmp_file_path).st_size
    tmp_time1 = time.time()
    print("文件上传用时：%.1fs" % (tmp_time1 - tmp_time0))
    print("文件上传速度：%.1fMB/s" % (file_size / 1024 / 1024 / (tmp_time1 - tmp_time0)))
    tmp_time0 = tmp_time1
    # 计算文件sha1
    file_checksum = get_file_sha1(tmp_file_path)
    tmp_time1 = time.time()
    print("计算sha1用时：%.1fs" % (tmp_time1 - tmp_time0))
    tmp_time0 = tmp_time1
    # 判断文件是否已存在，如果存在，直接秒传
    file = File.query.filter_by(checksum=file_checksum).first()
    if file:
        print("文件已存在，秒传")
        file_obj = File(id=file_id, checksum=file_checksum, user="test_user", filename=filename, mimetype=mimetype, size=file_size)
        db.session.add(file_obj)
        blocks = Block.query.filter_by(file_id=file.id)
        for block in blocks:
            block_obj = Block(id=str(uuid.uuid1()), checksum=block.checksum, file_id=file_id, seq=block.seq, virtual_node_id=block.virtual_node_id,
                              replica_number=block.replica_number)
            db.session.add(block_obj)
        db.session.commit()
        os.remove(tmp_file_path)
        return file_id, 203
    # 如果文件不存在，则分块上传到存储节点 TODO：多进程
    with open(tmp_file_path, "rb") as f:
        seq = 0
        for block_data in read_local_file_blocks(f):
            # 判断块是否已存在
            checksum = hashlib.sha1(block_data).hexdigest()
            exist_blocks = Block.query.filter_by(checksum=checksum).all()
            # 如果块已存在，则跳过上传
            if exist_blocks:
                print("块已存在，跳过")
                for exist_block in exist_blocks:
                    block_obj = Block(id=str(uuid.uuid1()), checksum=checksum, file_id=file_id, seq=seq, virtual_node_id=exist_block.virtual_node_id,
                                      replica_number=exist_block.replica_number)
                    db.session.add(block_obj)
                db.session.commit()
            # 否则上传块
            else:
                node_list = select_node(checksum)
                print(checksum, node_list)
                # 每个块需要上传 REPLICAS 次
                for virtual_node, node in node_list:
                    post_url = f"http://{node.address}/api/block/{checksum}"
                    t0 = time.time()
                    try:
                        res = requests.post(url=post_url, files={'file': block_data})
                        t1 = time.time()
                        print("分片上传用时：%.1fs" % (t1 - t0))
                        print("分片上传速度：%.1fMB/s" % (len(block_data) / 1024 / 1024 / (t1 - t0)))
                        print(res.status_code, res.text)
                        if res.status_code == 203:  # 上传成功
                            block_obj = Block(id=str(uuid.uuid1()), checksum=checksum, file_id=file_id, seq=seq, virtual_node_id=virtual_node.id,
                                              replica_number=virtual_node.replica_number)
                            db.session.add(block_obj)
                            db.session.commit()
                        else:
                            os.remove(tmp_file_path)
                            print("上传失败，存储节点返回值不为203，忽略该副本，等集群降级重建", res.status_code)
                            node.is_alive = False
                            node.offline_time = time.time()
                            for _virtual_node in VirtualNode.query.filter_by(node_id=node.id).all():
                                _virtual_node.is_alive = False
                            block_obj = Block(id=str(uuid.uuid1()), checksum=checksum, file_id=file_id, seq=seq, virtual_node_id=virtual_node.id,
                                              replica_number=virtual_node.replica_number)
                            db.session.add(block_obj)
                            db.session.commit()
                    except Exception as e:
                        print("上传失败，设置存储节点为掉线，忽略该副本，等集群降级重建", e)
                        node.is_alive = False
                        node.offline_time = time.time()
                        for _virtual_node in VirtualNode.query.filter_by(node_id=node.id).all():
                            _virtual_node.is_alive = False
                        block_obj = Block(id=str(uuid.uuid1()), checksum=checksum, file_id=file_id, seq=seq, virtual_node_id=virtual_node.id,
                                          replica_number=virtual_node.replica_number)
                        db.session.add(block_obj)
                        db.session.commit()
            seq += 1
    file_obj = File(id=file_id, checksum=file_checksum, user="test_user", filename=filename, mimetype=mimetype, size=file_size)
    db.session.add(file_obj)
    db.session.commit()
    os.remove(tmp_file_path)
    tmp_time1 = time.time()
    print("上传到存储节点用时：%.1fs" % (tmp_time1 - tmp_time0))
    print("总用时：%.1fs" % (tmp_time1 - start_time))
    return file_id, 203


@app.route("/api/file/<string:file_id>", methods=['DELETE'])
def delete_file(file_id: str):
    """
    删除文件
    :param file_id: 文件id
    :return: 文件不存在返回404，删除成功返回204
    """
    # 判断文件是否存在
    file = File.query.filter_by(id=file_id).first()
    if not file:
        return "File not found", 404
    # 删除所有块
    blocks = Block.query.filter_by(file_id=file_id).order_by(Block.seq).all()
    print(blocks)
    for block in blocks:
        # 先判断这个块有没有被别的文件使用，如果有，仅删库，不删文件
        exist_blocks = Block.query.filter_by(checksum=block.checksum).all()
        if len(exist_blocks) > REPLICAS:
            print("这个块还有别的文件使用，跳过删除")
            db.session.delete(block)
            db.session.commit()
            continue
        # 如果没被使用，则找到对应的节点发删除请求
        virtual_node = VirtualNode.query.filter_by(id=block.virtual_node_id).first()
        if virtual_node.is_alive:
            node = Node.query.filter_by(id=virtual_node.node_id).first()
            url = f"http://{node.address}/api/block/{block.checksum}"
            try:
                res = requests.delete(url=url)
                if res.status_code != 204 and res.status_code != 404:
                    print("Delete block error:", node, res.status_code)
                    print(res.status_code, res.text)
            except Exception as e:
                print("Delete block error:", node, e)
        else:
            print("节点不在线，忽略")
        db.session.delete(block)
        db.session.commit()
    db.session.delete(file)
    db.session.commit()
    return "", 204


@app.route("/api/node", methods=['GET'])
def get_nodes():
    """
    列出所有节点
    :return: 节点列表
    """
    output = ""
    nodes = Node.query.all()
    for node in nodes:
        output += str(node) + "\n"
    return output


@app.route("/api/node/<string:node_id>", methods=['GET'])
def get_node(node_id: str):
    """
    获取单个节点的信息
    :param node_id: 节点id
    :return: 节点信息以及该节点的所有虚拟节点
    """
    output = ""
    node = Node.query.filter_by(id=node_id).first()
    if node:
        output += str(node)
        virtual_nodes = VirtualNode.query.filter_by(node_id=node_id).all()
        for virtual_node in virtual_nodes:
            output += "\n" + str(virtual_node)
        return output
    return "Node not found", 404


@app.route("/api/node/<string:node_name>/<string:node_port>/<int:disk_space>", methods=['POST'])
def add_node(node_name: str, node_port: str, disk_space: int):
    """
    新增节点 TODO: 并发添加会导致都加到同一个replica_num的bug
    :param node_name: 节点名字
    :param node_port: 节点端口
    :param disk_space: 节点磁盘空间
    :return: 添加成功返回203，失败返回400
    """
    node_ip = request.remote_addr
    node_address = f"{node_ip}:{node_port}"
    print(node_name, node_address, disk_space)
    # 访问node，看看是否是通的
    res = requests.get(f"http://{node_address}/api/status", timeout=TIMEOUT)
    if res.status_code != 200:
        return "node dead", 400
    # 如果node存在则刷新心跳，并更新address
    node = Node.query.filter_by(id=node_name).first()
    if node:
        node.address = node_address  # 更新地址，用于处理动态ip的情况
        # 如果节点复活了，则更新数据库
        if not node.is_alive:
            node.is_alive = True
            node.offline_time = 0
            virtual_nodes = VirtualNode.query.filter_by(node_id=node.id)
            for virtual_node in virtual_nodes:
                virtual_node.is_alive = True
        db.session.commit()
        return node_ip, 203
    # 如果node不存在则新增Node和VirtualNode
    replica_number = select_replica()
    node = Node(id=node_name, address=node_address, is_alive=True, disk_space=disk_space, replica_number=replica_number)
    db.session.add(node)
    for seq in range(0, disk_space * DEFAULT_VIRTUAL_NODE_PER_GB):
        virtual_node_name = f"{node_name}.{seq}"
        checksum = hashlib.sha1(virtual_node_name.encode("utf8")).hexdigest()
        virtual_node = VirtualNode(id=checksum, seq=seq, node_id=node_name, is_alive=True, replica_number=replica_number)
        db.session.add(virtual_node)
    db.session.commit()
    return node_ip, 203


@app.route("/api/node/<string:node_id>", methods=['DELETE'])
def delete_node(node_id: str):  # TODO 。。。
    return node_id, 204


@app.route("/api/cluster")
def cluster_status():
    """返回集群当前状态"""
    global CLUSTER_STATUS
    output = "集群当前状态：" + CLUSTER_STATUS + "\n"
    for replica_number in range(0, REPLICAS):
        nodes = Node.query.filter_by(replica_number=replica_number)
        total_disk_space = sum(list(map(lambda x: x.disk_space, nodes)))
        output += f"存储容量（副本{replica_number}）：{total_disk_space} GB\n"
    return output, 200


def read_local_file_blocks(f):
    """
    返回一个块数据的生成器
    :param f: 文件对象
    :return: 块数据
    """
    while True:
        block = f.read(BLOCK_SIZE)
        if not block:
            break
        yield block


def read_remote_file_blocks(file_id):
    """
    返回一个读取文件的生成器
    :param file_id: 文件的id（假设文件一定存在）
    :return: 块数据
    """
    blocks = Block.query.filter_by(file_id=file_id).all()
    max_seq = max(list(map(lambda x: x.seq, blocks)))
    for seq in range(0, max_seq + 1):
        blocks = Block.query.filter_by(file_id=file_id, seq=seq).all()
        random.shuffle(blocks)  # 打乱节点，随机选择一个replica
        for block in blocks:
            virtual_node = VirtualNode.query.filter_by(id=block.virtual_node_id).first()
            node = Node.query.filter_by(id=virtual_node.node_id).first()
            get_url = f"http://{node.address}/api/block/{block.checksum}"
            # 尝试下载，一个节点故障不行就下一个
            try:
                res = requests.get(url=get_url, timeout=TIMEOUT)
                if res.status_code != 200:
                    continue
            except Exception as e:
                print("节点请求故障：", get_url, e)
                continue
            # 下载成功返回内容
            yield res.content
            break


def select_node(checksum, replica=None):
    """
    根据checksum选择对应的虚拟存储节点和存储节点
    :param checksum: 块checksum
    :param replica: 副本序号，如果为空则计算所有副本
    :return: replica is None: [(虚拟存储节点对象, 存储节点对象), (...,...)] replica is not None: (虚拟存储节点对象, 存储节点对象)
    """
    if replica is None:
        node_list = []
        for replica_number in range(0, REPLICAS):
            virtual_nodes = VirtualNode.query.filter_by(replica_number=replica_number, is_alive=True).order_by(VirtualNode.id).all()
            node = Node.query.filter_by(id=virtual_nodes[0].node_id).first()
            node_item = (virtual_nodes[0], node)  # 默认节点
            for virtual_node in virtual_nodes:
                if virtual_node.id < checksum:
                    continue
                node = Node.query.filter_by(id=virtual_node.node_id).first()
                node_item = (virtual_node, node)
                break
            node_list.append(node_item)
        return node_list
    else:
        virtual_nodes = VirtualNode.query.filter_by(replica_number=replica, is_alive=True).order_by(VirtualNode.id).all()
        node = Node.query.filter_by(id=virtual_nodes[0].node_id).first()
        node_item = (virtual_nodes[0], node)  # 默认节点
        for virtual_node in virtual_nodes:
            if virtual_node.id < checksum:
                continue
            node = Node.query.filter_by(id=virtual_node.node_id).first()
            node_item = (virtual_node, node)
        return node_item


def select_replica():
    """
    选择磁盘总容量最小的副本区
    :return: 副本区数字
    """
    replica_disk_space = []
    for replica_number in range(0, REPLICAS):
        nodes = Node.query.filter_by(replica_number=replica_number)
        total_disk_space = sum(list(map(lambda x: x.disk_space, nodes)))
        replica_disk_space.append(total_disk_space)
    return replica_disk_space.index(min(replica_disk_space))


def get_file_sha1(file_path):
    """
    计算一个文件的sha1
    :param file_path: 文件路径
    :return: checksum
    """
    m = hashlib.sha1()
    with open(file_path, "rb") as f:
        while True:
            data = f.read(1024 * 1024)
            if not data:
                break
            m.update(data)
    return m.hexdigest()


def cluster_health_scan():
    """定时扫描节点状态，如果挂了则更新数据库，并设置降级"""
    global CLUSTER_STATUS, LAST_RESTORE_TIME
    while True:
        # 检查好的机器
        nodes = Node.query.filter_by(is_alive=True).all()
        for node in nodes:
            try:
                res = requests.get(f"http://{node.address}/api/status", timeout=TIMEOUT)
                if res.status_code != 200:
                    print("node dead：", node, res.status_code)
                    node.is_alive = False
                    node.offline_time = int(time.time())
                    for virtual_node in VirtualNode.query.filter_by(node_id=node.id).all():
                        virtual_node.is_alive = False
            except Exception as e:
                print("node dead：", node, e)
                node.is_alive = False
                node.offline_time = int(time.time())
                for virtual_node in VirtualNode.query.filter_by(node_id=node.id).all():
                    virtual_node.is_alive = False
        db.session.commit()
        time.sleep(10)
        # 检查挂了的机器
        nodes = Node.query.filter_by(is_alive=False).all()
        if nodes:
            for node in nodes:
                try:
                    res = requests.get(f"http://{node.address}/api/status", timeout=TIMEOUT)
                    if res.status_code == 200:
                        node.is_alive = True
                        node.offline_time = 0
                        for virtual_node in VirtualNode.query.filter_by(node_id=node.id).all():
                            virtual_node.is_alive = True
                except Exception as e:
                    pass
            db.session.commit()
        # 当集群是稳定状态时，如果还有机器挂的就设置降级
        if CLUSTER_STATUS == "stable":
            nodes = Node.query.filter_by(is_alive=False).all()
            for node in nodes:  # 如果机器挂的时间是上次恢复后的，就降级，否则忽略
                if node.offline_time >= LAST_RESTORE_TIME:
                    CLUSTER_STATUS = "degraded"
                    print("集群降级")
                    break
            else:
                CLUSTER_STATUS = "stable"


def cluster_recovery():
    """定期判断集群状态，如果是降级状态，则开始循环迁移。将下线设备忽略，重新计算hash分布，调存储节点api进行迁移。"""
    global CLUSTER_STATUS, LAST_RESTORE_TIME
    while True:
        if CLUSTER_STATUS in ["degraded", "failed"]:
            all_migrate_success = True
            # 遍历挂掉的节点
            virtual_nodes = VirtualNode.query.filter_by(is_alive=False).all()
            for virtual_node in virtual_nodes:
                # 遍历需要迁移的块
                danger_blocks = Block.query.filter_by(virtual_node_id=virtual_node.id).all()
                if not danger_blocks:
                    continue
                replica = virtual_node.replica_number
                for danger_block in danger_blocks:
                    migrate_success = False
                    replica_blocks = Block.query.filter_by(checksum=danger_block.checksum)
                    replica_virtual_nodes = set(map(lambda x: x.virtual_node_id, replica_blocks))
                    # 选择一个可用的节点进行迁移，如果失败则用下一个节点
                    for replica_virtual_node in replica_virtual_nodes:
                        replica_node = Node.query.filter_by(id=VirtualNode.query.filter_by(id=replica_virtual_node).first().node_id,
                                                            is_alive=True).first()
                        target_virtual_node, target_node = select_node(danger_block.checksum, replica)
                        if not replica_node:
                            continue
                        try:
                            res = requests.post(f"http://{target_node.address}/api/sync_block/{replica_node.address}/{danger_block.checksum}")
                            if res.status_code == 204:
                                migrate_success = True
                                danger_block.virtual_node_id = target_virtual_node.id
                                break
                            print("迁移失败：", replica_node, "->", target_node, res.status_code)
                        except Exception as e:
                            print("迁移失败：", replica_node, "->", target_node, e)
                    if not migrate_success:
                        all_migrate_success = False
                        print("迁移失败：", danger_block)
            db.session.commit()
            if all_migrate_success:
                CLUSTER_STATUS = "stable"
                LAST_RESTORE_TIME = time.time()
                print("集群已恢复")
            else:
                CLUSTER_STATUS = "failed"
                print("有部分块已丢失，集群未能完全恢复")
        time.sleep(10)


if __name__ == '__main__':
    db.create_all()
    # 创建集群状态监控线程
    cluster_health_scan_thread = threading.Thread(target=cluster_health_scan)
    cluster_health_scan_thread.start()
    # 创建集群降级恢复线程
    cluster_recovery_thread = threading.Thread(target=cluster_recovery)
    cluster_recovery_thread.start()
    # 启动服务器
    app.run(host="0.0.0.0", port=PORT)
