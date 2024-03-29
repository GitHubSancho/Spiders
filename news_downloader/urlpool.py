#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: urlpool.py
#CREATE_TIME: 2022-07-25
#AUTHOR: Sancho
"""
网址池管理器
"""

from ast import Str
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
    def __init__(self, db, config, path):
        self.db = UrlDB(db)
        self.cfg = config
        self.path = path
        self.hubs = self.cfg["hubs.hubs"]
        self.pool = []
        """
        self.pool = 
        [{
            "host": "news.baidu.com",
            "url": "http://news.baidu.com/12345.html",
            "status": "pending/waiting",
            "mode": "url/hub",
            "pendedtime": 0,
            "failure": 0
        }, {
            "host": "sport.baidu.com",
            "url": "http://news.baidu.com/54321.html",
            "status": "pending/waiting",
            "mode": "url/hub",
            "pendedtime": 0,
            "failure": 0
        }]
        """
        self.set_hubs(self.hubs)
        self._load_cache()  # 读取上次未完成抓取网址的数据

    def _load_cache(self):
        """读取上次未完成抓取网址的数据"""
        path = self.path + '.pkl'
        try:
            with open(path, 'rb') as f:
                self.pool = pickle.load(f)
                print('load:{}'.format(self.pool))
        except:
            pass

    def _dump_cache(self):
        """写入缓存"""
        path = self.path + '.pkl'
        try:
            with open(path, 'wb') as f:
                pickle.dump(self.pool, f)  # 将未完成抓取的网址，序列化写入硬盘
            print('saved!')
        except:
            pass

    def __del__(self):
        """退出时自动调用写入缓存"""
        self._dump_cache()
        self.cfg.close()

    def _dump_cfg(self, hub):  # 写入配置文件
        # 将hub链接写入配置文件
        if hub not in self.cfg["hubs.hubs"]:
            self.hubs.append(hub)
            with open(self.path + '_hubs.py', 'w+', encoding='utf-8') as f:
                f.write('hubs:%s' % str(self.hubs))
            return True

        return

    def set_hubs(self, urls: list):
        """设置首页中的链接到网址池"""
        self.addmany(urls, mode="hub")
        if type(urls) == Str:
            self._dump_cfg(urls)
        else:
            for url in urls:
                self._dump_cfg(url)

    def set_status(self, url, status_code):
        """访问链接后设置状态"""
        # 简单判断主机地址是否合法
        host = urlparse.urlparse(url).netloc  # 解析到主机地址
        if not host or '.' not in host:
            print('地址错误:', url, ', len of ur:', len(url))
            return False
        _status = [d for d in self.pool if d['url'] == url]  # url是否存在pool中
        if _status == []:
            return False
        _status = _status[0]  # 去掉列表外壳：取到字典数据
        _index = self.pool.index(_status)  # 取出链接在pool中的索引
        # 根据链接状态保存到服务器
        mode = _status["mode"]
        if status_code == 200 and mode == 'url':
            self.db.set_success(url)  # 写入到成功的网址池
            self.pool.pop(_index)  # 下载完成删除url
            return True
        elif status_code == 404:
            self.db.set_failure(url)  # （地址不存在）写入到失败的网址池
            self.pool.pop(_index)
            return True
        # 访问失败时的处理
        if _status['failure'] > 0:  # 是否失败过
            self.pool[_index]['failure'] += 1  # 记录失败次数+1
            if mode == 'url' and _status['failure'] > self.cfg[
                    'failure_threshold']:  # 判断失败次数是否超过上限
                self.db.set_failure(url)  # 从数据库中设置失败状态
                self.pool.pop(_index)
            else:  # 没有达到失败上限
                self.pool[_index]['status'] = 'waiting'
        else:  # 第一次失败
            self.pool[_index]['failure'] = 1
            self.pool[_index]['status'] = 'waiting'

        return True

    def add(self, url, mode="url"):
        """将链接添加到网址池"""
        # 简单判断主机地址是否合法
        host = urlparse.urlparse(url).netloc  # 解析到主机地址
        if not host or '.' not in host:  # 简单判断主机地址是否合法
            print('地址错误:', url, ', len of ur:', len(url))
            return False
        if mode == 'url' and self.db.has(url):  # 是否存在数据库中
            return False

        # 遍历寻找url是否存在pool中
        for d in self.pool:
            if d.get(url, 0):  # 取出字典
                if time.time() - d["pendedtime"] > self.cfg[
                        'pending_threshold']:  # 判断是否超过刷新时间
                    d["pendedtime"] = 0
                    d["status"] = 'waiting'
                    return True
                return
        # url不存在pool中则：添加到pool中
        self.pool.append({
            "host": host,
            "url": url,
            "status": "waiting",
            "mode": mode,
            "pendedtime": 0,
            "failure": 0
        })

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
        hosts = []
        hubs_count = count * hub_percent // 100  # 计算需要弹出的hub_url个数
        urls_count = count - hubs_count

        for d in self.pool:
            # 判断host是否被采集过（限制每次访问的子域名均不相同）
            host = urlparse.urlparse(d['url']).netloc  # 解析到主机地址
            if limit and host in hosts:
                continue

            #  判断url状态：成功则记录url
            _refresh = [
                time.time() - d['pendedtime'] > self.cfg['hub_refresh_span'],
                time.time() - d['pendedtime'] > self.cfg['pending_threshold']
            ]  # hub/url是否超过了刷新时间
            _count = [len(hubs) < hubs_count,
                      len(urls) < urls_count]  # hub/url长度是否在范围内
            if d['mode'] == 'hub' and _refresh[0] and _count[0]:
                hubs.append([d['url'], 1])
                hosts.append(host)
                d['pendedtime'] = time.time()  # 更新取出时间
            elif d['mode'] == 'url' and _refresh[1] and _count[1]:
                urls.append([d['url'], 0])
                hosts.append(host)
                d['pendedtime'] = time.time()

            # 取出个数已满：退出循环
            if not _count[0] and not _count[1]:
                break

        return hubs + urls

    def size(self):
        """返回链接数"""
        return len(self.pool)

    # def empty(self, ):
    #     """查看待爬取的地址池是否为空"""
    #     return self.waiting_count == 0
