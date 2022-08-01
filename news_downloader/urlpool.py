#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: urlpool.py
#CREATE_TIME: 2022-07-25
#AUTHOR: Sancho
"""
网址池
"""

from multiprocessing import pool
import pickle
import time
import json
import sys
import urllib.parse as urlparse
from urldb import UrlDB


class UrlPool:
    '''
    用于管理url的网址池
    '''
    def __init__(self,
                 *args: str,
                 host='127.0.0.1',
                 database='demo',
                 collection='test001',
                 user=None,
                 password=None,
                 port=27017,
                 max_idle_time=7 * 3600,
                 timeout=10,
                 time_zone=None):
        self.name = database
        self.path = sys.path[0] + '\\' + self.name  # 当前文件路径
        self.db = UrlDB(host=host,
                        database=database,
                        collection=collection,
                        user=user,
                        password=password,
                        port=port,
                        max_idle_time=max_idle_time,
                        timeout=timeout,
                        time_zone=time_zone)  # 载入网址数据库
        self.pool = {}
        """
        self.pool = {
            "host1": {
                "url1": {
                    "status":"pending"
                    "pendedtime": 0,
                    "failure": 0,
                    "mode": "url/hub"
                },
                "url2": {
                    "status":"waiting"
                    "pendedtime": 0,
                    "failure": 0,
                    "mode": "url/hub"
                }
            },
            "host2": {
                "url1": {
                    "status":"pending"
                    "pendedtime": 0,
                    "failure": 0,
                    "mode": "url/hub"
                },
                "url1": {
                    "status":"waiting"
                    "pendedtime": 0,
                    "failure": 0,
                    "mode": "url/hub"
                }
            }
        }
        """
        self._load_ini()  # 读取配置文件
        self._load_cache()  # 读取上次未完成抓取网址的数据

    def _load_ini(self):
        """读取配置文件"""
        path = self.path + '.json'  # 当前文件目录下
        try:
            with open(path, 'r') as f:
                self.ini = json.load(f)
                print("ini:")
                print(self.ini)
        except FileNotFoundError:
            with open(path, 'w+') as f:
                myini = {
                    "pending_threshold": 10,
                    "failure_threshold": 3,
                    "hub_refresh_span": 10
                }
                json.dump(myini, f)
                self.ini = json.loads(myini)
        except:
            pass

    def __del__(self):
        """退出时自动调用写入缓存"""
        self._dump_cache()

    def _load_cache(self, ):
        """读取上次未完成抓取网址的数据"""
        path = self.path + '.pkl'
        try:
            with open(path, 'rb') as f:
                self.pool = pickle.load(f)
                print('load:')
                print(self.pool)
        except:
            pass

    def _dump_cache(self):
        """写入缓存"""
        path = self.path + '.pkl'
        try:
            with open(path, 'wb') as f:
                for host in self.pool.keys():  # 删除不保存的值
                    for url in self.pool[host]:
                        if self.pool[host][url]['status'] != 'waiting':
                            self.pool[host].pop(url)
                pickle.dump(self.pool, f)  # 将未完成抓取的网址，序列化写入硬盘
            print('saved!')
        except:
            pass

    def set_hubs(self, urls):
        """设置首页中的链接到网址池"""
        self.addmany(urls, mode="hub")

    def set_status(self, url, status_code):
        """访问链接并设置状态"""
        host = urlparse.urlparse(url).netloc  # 解析到主机地址
        if not host or '.' not in host:  # 简单判断主机地址是否合法
            print('地址错误:', url, ', len of ur:', len(url))
            return False
        if url not in self.pool['host']:
            return False

        mode = self.pool[host][url]['mode']
        if status_code == 200 and mode == 'url':
            self.db.set_success(url)  # 写入到成功的网址池
            self.pool[host].pop(url)  # 下载完成删除url
            return
        elif status_code == 404:
            self.db.set_failure(url)  # （地址不存在）写入到失败的网址池
            self.pool[host].pop(url)  # 删除url
            return

        if self.pool[host][url]['failure'] > 0:  # 其它状态时判断是否已经在失败的网址池中
            self.pool[host][url]['failure'] += 1  # 记录失败次数+1
            if mode == 'url' and self.pool[host][url]['failure'] > self.ini[
                    'failure_threshold']:  # 判断失败次数是否超过上限
                self.db.set_failure(url)  # 从数据库中设置失败状态
                self.pool[host].pop(url)  # 删除url
            else:  # 没有达到失败上限
                self.pool[host][url]['status'] = 'waiting'
        else:  # 第一次失败
            self.pool[host][url]['failure'] = 1
            self.pool[host][url]['status'] = 'waiting'

    def add(self, url, mode="url"):
        """将链接添加到网址池"""
        host = urlparse.urlparse(url).netloc  # 解析到主机地址
        if not host or '.' not in host:  # 简单判断主机地址是否合法
            print('地址错误:', url, ', len of ur:', len(url))
            return False
        if mode == 'url' and self.db.has(url):  # 是否存在数据库中
            return
        if self.pool.get(host, 0):  # 判断主机地址是否已被记录
            if self.pool[host].get(url, 0):
                if time.time() - self.pool[host][url]["pendedtime"] > self.ini[
                        'pending_threshold']:
                    self.pool[host][url]["pendedtime"] = 0
                    self.pool[host][url]["status"] = 'waiting'
                    return
                return
            self.pool[host].update({
                url: {
                    "status": "waiting",
                    "pendedtime": 0,
                    "failure": 0,
                    "mode": mode
                }
            })  # 加入待下载队列
        else:
            self.pool[host] = {
                url: {
                    "status": "waiting",
                    "pendedtime": 0,
                    "failure": 0,
                    "mode": mode
                }
            }  # 加入待下载队列
        return True

    def addmany(self, urls, mode='url'):
        """将多个链接添加到网址池"""
        if isinstance(urls, str):  # 判断是否是字符串类型
            # print('urls是字符串,请传入多个链接的可迭代对象！', urls)
            self.add(urls, mode)
        else:
            for url in urls:
                self.add(url, mode)  # 遍历将url添加到网址池

    def pop(self, count, hub_percent=50, limit=True):
        """
        弹出链接进入下载
        count:需要弹出的链接个数（并发）
        hub_percent:弹出的链接中hub_url的占比
        return:{urls,hubs}
        """
        hubs = []
        urls = []
        hub_count = count * hub_percent // 100  # 计算需要弹出的hub_url个数
        hosts = self.pool.keys()

        for host in hosts:
            for url in self.pool[host]:
                mode = self.pool[host][url].get('mode', 0)
                status = self.pool[host][url].get('status', 0)
                pendedtime = self.pool[host][url].get('pendedtime', 0)

                if mode == 'hub' and time.time(
                ) - pendedtime > self.ini["hub_refresh_span"]:  # 超过刷新时间
                    if len(hubs) < hub_count:
                        hubs.append(url)
                        self.pool[host][url]['pendedtime'] = time.time(
                        )  # 更新取出时间
                        if limit == True:  # 限制同一host下每次只有一个链接，避免给服务器造成过多压力
                            break

                if mode == 'url' and time.time(
                ) - pendedtime > self.ini["pending_threshold"]:  #
                    if len(urls) < count - hub_count:
                        urls.append(url)
                        self.pool[host][url]['pendedtime'] = time.time(
                        )  # 更新取出时间
                        if limit == True:
                            break

            if len(hubs) + len(urls) >= count:
                break
        return hubs + urls

    def size(self, ):
        """返回链接数"""
        return len(
            [url for host in self.pool.keys() for url in self.pool[host]])

    # def empty(self, ):
    #     """查看待爬取的地址池是否为空"""
    #     return self.waiting_count == 0


if __name__ == "__main__":
    urlpool = UrlPool()
    urlpool.set_hubs(
        ['https://sports.sina.com.cn/', 'https://news.sina.com.cn/china/'])
    urlpool.addmany([
        'https://news.sina.com.cn/c/2022-07-26/doc-imizirav5396532.shtml',
        'https://news.sina.com.cn/s/2022-07-26/doc-imizirav5429097.shtml',
        'https://news.sina.com.cn/gov/xlxw/2022-07-26/doc-imizmscv3572469.shtml'
    ])
    print("main:")
    print(urlpool.pool)
    print("pop:")
    print(urlpool.pop(5))
    print("size:", urlpool.size())
    del urlpool
