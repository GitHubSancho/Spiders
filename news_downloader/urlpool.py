#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: urlpool.py
#CREATE_TIME: 2022-07-25
#AUTHOR: Sancho
"""
网址池实现
"""

from multiprocessing import pool
import pickle
import pymongo
import time
import json
import sys
import urllib.parse as urlparse
from urldb import UrlDB


class UrlPool:
    '''
    用于管理url的网址池
    '''
    def __init__(self, db_name, host, port):
        self.name = db_name
        self.path = sys.path[0] + '\\' + self.name
        self.db = UrlDB(db_name, host, port)  # 载入网址数据库

        # self.waiting = {}  # {host: set([urls]), } 按host分组，记录等待下载的URL
        # self.pending = {
        # }  # {url: pended_time, } 记录已被取出（self.pop()）但还未被更新状态（正在下载）的URL
        # self.hub_pool = {}  # {url: last_query_time, }  存放hub url（首页的链接）
        # self.failure = {}  # {url: times,} 记录失败的URL的次数

        # self.failure_threshold = 3  # 允许的最大失败次数
        # self.pending_threshold = 10  # pending的最大时间，过期要重新下载
        # self.waiting_count = 0  # self.waiting 字典里面的url的个数
        self.max_hosts = ['', 0]  # [host: url_count] 目前pool中url最多的host及其url数量

        # self.hub_refresh_span = 0  # 爬取的时间频率

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

        self.load_cache()  # 读取上次未完成抓取网址的数据
        self.load_ini()

    def __del__(self):
        """退出时自动调用写入缓存"""
        self.dump_cache()

    def load_cache(self, ):
        """读取上次未完成抓取网址的数据"""
        path = self.path + '.pkl'
        try:
            with open(path, 'rb') as f:
                self.pool = pickle.load(f)
                print('load:')
                print(self.pool)

            # cc = len([
            #     url for host in self.pool.keys() for url in self.pool[host]
            # ])  # 数据字段计数
            # print('加载到网址池的网址:', sum(cc))
        except:
            pass

    def dump_cache(self):
        """写入缓存"""
        path = self.path + '.pkl'
        try:
            with open(path, 'wb') as f:
                # waiting = {
                #     url: self.pool[host][url]
                #     for host in self.pool.keys() for url in self.pool[host]
                #     if self.pool[host][url]['status'] == 'waiting'
                # }
                for host in self.pool.keys():
                    for url in self.pool[host]:
                        if self.pool[host][url]['status'] != 'waiting':
                            self.pool[host].pop(url)
                pickle.dump(self.pool, f)  # 将未完成抓取的网址，序列化写入硬盘
            print('saved!')
        except:
            pass

    def load_ini(self):
        """读取配置文件"""
        path = self.path + '.json'  # 当前文件目录下
        try:
            with open(path, 'r') as f:
                self.ini = json.load(f)
                print(self.ini)
        except FileNotFoundError:
            with open(path, 'w+') as f:
                myini = {"pending_threshold": 10, "failure_threshold": 3}
                json.dump(myini, f)
                self.ini = json.loads(myini)
        except:
            pass

    def set_hubs(self, urls):
        """设置首页中的链接到网址池"""
        # self.hub_refresh_span = hub_refresh_span  # 刷新时间
        # self.hub_pool = {}
        # for url in urls:
        #     self.hub_pool[url] = 0

        self.addmany(urls, mode="hub")

    def set_status(self, url, status_code):
        """访问链接并设置状态"""
        # if url in self.pending:
        #     self.pending.pop(url)  # 取出已下载的、待更新的url

        # if status_code == 200:
        #     self.db.set_success(url)  # 写入到成功的网址池
        #     return
        # if status_code == 404:
        #     self.db.set_failure(url)  # （地址不存在）写入到失败的网址池
        #     return
        # if url in self.failure:  # 其它状态时判断是否已经在失败的网址池中
        #     self.failure[url] += 1  # 记录失败次数+1
        #     if self.failure[url] > self.failure_threshold:  # 判断失败次数是否超过上限
        #         self.db.set_failure(url)  # 从数据库中设置失败状态
        #         self.failure.pop(url)  # 从失败的网址池中销毁
        #     else:  # 没有达到失败上限
        #         self.add(url)  # 重新加载（放入self.waittig）
        # else:  # 第一次失败
        #     self.failure[url] = 1
        #     self.add(url)  # 重新加载（放入self.waittig）

        host = urlparse.urlparse(url).netloc  # 解析到主机地址
        if not host or '.' not in host:  # 简单判断主机地址是否合法
            print('地址错误:', url, ', len of ur:', len(url))
            return False
        self.pool[host].pop(url)

        if status_code == 200:
            self.db.set_success(host, url)  # 写入到成功的网址池
            return
        if status_code == 404:
            self.db.set_failure(host, url)  # （地址不存在）写入到失败的网址池
            return
        if self.pool[host][url]['failure'] > 0:  # 其它状态时判断是否已经在失败的网址池中
            self.pool[host][url]['failure'] += 1  # 记录失败次数+1
            if self.pool[host][url]['failure'] > self.ini[
                    'failure_threshold']:  # 判断失败次数是否超过上限
                self.db.set_failure(host, url)  # 从数据库中设置失败状态
                self.failure.pop(url)  # 从失败的网址池中销毁
            else:  # 没有达到失败上限
                # self.add(url)  # 重新加载（放入self.waittig）
                self.pool[host][url]['status'] = 'waiting'
        else:  # 第一次失败
            self.pool[host][url]['failure'] = 1
            # self.add(url)  # 重新加载（放入self.waittig）
            self.pool[host][url]['status'] = 'waiting'

    # def _push_to_pool(self, url):
    #     """将url按host分组放入self.waiting"""
    # host = urlparse.urlparse(url).netloc  # 解析到主机地址
    # if not host or '.' not in host:  # 判断主机地址是否合法
    #     print('地址错误:', url, ', len of ur:', len(url))
    #     return False
    # if host in self.waiting:  # 判断主机地址是否已被记录
    #     if url in self.waiting[host]:  # 判断链接是否已经在待下载池中
    #         return True
    #     self.waiting[host].add(url)  # 添加链接到待下载池中
    #     if len(self.waiting[host]) > self.max_hosts[1]:  # 判断host下的链接是否是最多的
    #         self.max_hosts[1] = len(self.waiting[host])  # 刷新host的最多数量
    #         self.max_hosts[0] = host
    # else:  # 如果不存在待下载队列（新链接）
    #     self.waiting[host] = set([url])  # 加入待下载队列
    # self.waiting_count += 1  # 计数器+1
    # return True

    def add(self, url, mode="url"):
        """将链接添加到网址池"""
        # if always:  # 强制放入待下载队列
        #     return self.push_to_pool(url)
        # pended_time = self.pending.get(url, 0)  # 查看上次下载的时间
        # if time.time() - pended_time < self.pending_threshold:  # 是否在刷新间隔
        #     print('正在下载:', url)
        #     return
        # if self.db.has(url):  # 链接是否在数据库
        #     return
        # if pended_time:  # 超过刷新间隔
        #     self.pending.pop(url)  # 弹出
        # return self.push_to_pool(url)  # 放入self.waiting

        host = urlparse.urlparse(url).netloc  # 解析到主机地址
        if not host or '.' not in host:  # 简单判断主机地址是否合法
            print('地址错误:', url, ', len of ur:', len(url))
            return False
        if mode == 'url' and self.db.has(host, url):  # 是否存在数据库中
            return
        if self.pool.get(host, 0):  # 判断主机地址是否已被记录
            if self.pool[host].get(url,
                                   {}).get("status",
                                           0) == "waiting":  # 判断链接是否已经在待下载池中
                return True
            elif time.time() - self.pool[host].get(url, {}).get(
                    "pendedtime",
                    0) < self.ini['pending_threshold']:  # 判断是否在刷新间隔中
                return
            self.pool[host].update({
                url: {
                    "status": "waiting",
                    "pendedtime": 0,
                    "failure": 0,
                    "mode": mode
                }
            })  # 添加链接到待下载池中
            # if len(self.pool[host]) > self.max_hosts[1]:  # 判断host下的链接是否是最多的
            #     self.max_hosts[1] = len(self.pool[host])  # 刷新host的最多数量
            #     self.max_hosts[0] = host
        else:  # 如果不存在待下载队列（新链接）
            self.pool[host] = {
                url: {
                    "status": "waiting",
                    "pendedtime": 0,
                    "failure": 0,
                    "mode": mode
                }
            }  # 加入待下载队列
        # self.waiting_count += 1  # 计数器+1
        return True

    def addmany(self, urls, mode='url'):
        """将多个链接添加到网址池"""
        if isinstance(urls, str):  # 判断是否是字符串类型
            # print('urls是字符串,请传入多个链接的可迭代对象！', urls)
            self.add(urls, mode)
        else:
            for url in urls:
                self.add(url, mode)  # 遍历将url添加到网址池

    def pop(self, count, hub_percent=50):
        """
        弹出链接进入下载
        count:需要弹出的链接个数（并发）
        hub_percent:弹出的链接中hub_url的占比
        return:{urls,hubs}
        """
        # print('\n\tmax of host:', self.max_hosts)

        # # 取出的url有两种类型：hub=1, 普通=0
        # url_attr_url = 0  # 0表示普通url
        # url_attr_hub = 1  # 1表示hub_url

        # # 1. 首先取出hub，保证获取hub里面的最新url.
        # hubs = {}
        # hub_count = count * hub_percent // 100  # 计算需要弹出的hub_url个数
        # for hub in self.hub_pool:
        #     span = time.time() - self.hub_pool[hub]  # 计算到上次弹出时的时间差
        #     if span < self.hub_refresh_span:  # 判断时间差是否在刷新间隔
        #         continue
        #     hubs[hub] = url_attr_hub
        #     self.hub_pool[hub] = time.time()  # 更新时间戳
        #     if len(hubs) >= hub_count:  # 计算弹出个数
        #         break

        # # 2. 再取出普通url
        # left_count = count - len(hubs)  # 计算需要弹出的普通url个数
        # urls = {}
        # for host in self.waiting:  # 遍历待下载的队列
        #     if not self.waiting[host]:  # 判断当前host下是否没有链接待爬取
        #         continue
        #     url = self.waiting[host].pop()  # 弹出url
        #     urls[url] = url_attr_url  # 标记为普通url
        #     self.pending[url] = time.time()  # 更新时间戳
        #     if self.max_hosts[0] == host:  # 判断是否是最多链接数的host
        #         self.max_hosts[1] -= 1
        #     if len(urls) >= left_count:  # 达到需要弹出的数量
        #         break
        # self.waiting_count -= len(urls)  # 更新计数器
        # print('To pop:%s, hubs: %s, urls: %s, hosts:%s' %
        #       (count, len(hubs), len(urls), len(self.waiting)))
        # urls.update(hubs)  # 合并urls和hubs两个字典
        # return urls

        hubs = []
        urls = []
        hub_count = count * hub_percent // 100  # 计算需要弹出的hub_url个数
        hosts = self.pool.keys()
        for host in hosts:
            for url in self.pool[host]:
                if self.pool[host][url].get(
                        'mode', 0
                ) == 'hub':  #and time.time() - self.pool[host][url].get(
                    #'pendedtime', 0) > self.ini["hub_refresh_span"]: # TODO: 运行时添加
                    if len(hubs) < hub_count:
                        hubs.append(url)
                        self.pool[host][url]['pendedtime'] = time.time(
                        )  # 更新取出时间
                if self.pool[host][url].get(
                        'mode', 0
                ) == 'url' and self.pool[host][url].get(
                        'status', 0
                ) == 'waiting':  # and time.time() - self.pool[host][url].get(
                    #'pendedtime', 0) > self.ini["pending_threshold"]: # TODO: 运行时添加
                    if len(urls) < count - hub_count:
                        urls.append(url)
                        self.pool[host][url]['pendedtime'] = time.time(
                        )  # 更新取出时间
            if len(hubs) + len(urls) >= count:
                break
        return hubs + urls

    # def size(self, ):
    #     """返回链接数"""
    #     return self.waiting_count

    # def empty(self, ):
    #     """查看待爬取的地址池是否为空"""
    #     return self.waiting_count == 0


if __name__ == "__main__":
    urlpool = UrlPool("demo", "127.0.0.1", "27017")
    urlpool.set_hubs(
        ['https://sports.sina.com.cn/', 'https://news.sina.com.cn/china/'])
    urlpool.addmany([
        'https://news.sina.com.cn/c/2022-07-26/doc-imizirav5396532.shtml',
        'https://news.sina.com.cn/s/2022-07-26/doc-imizirav5429097.shtml',
        'https://news.sina.com.cn/gov/xlxw/2022-07-26/doc-imizmscv3572469.shtml'
    ])
    print(urlpool.pool)
    del urlpool
    # print(urlpool.pool)
    # print(urlpool.pop(5))
