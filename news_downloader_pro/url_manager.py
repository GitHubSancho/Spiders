#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: url_manager.py
#CREATE_TIME: 2022-08-07
#AUTHOR: Sancho
"""url状态管理器"""

import asyncio
import contextlib
import urllib.parse as urlparse
import pickle
import time
import yaml


class Url_Pool:
    def __init__(self, path, db, cfg) -> None:
        self.path = path
        self.db = db
        self.cfg = cfg
        self.url_pool = []

        self._load_cache()
        self._load_hubs()
        self.set_hubs(self.hubs)

    def _dump_hubs(self):
        """写到配置文件"""
        path = f'{self.path}_hubs.yml'
        with contextlib.suppress(Exception):
            with open(path, 'w') as f:
                yaml.dump({'hubs': self.hubs}, f)
            print('saved hubs!')

    def _dump_cache(self):
        """写到缓存"""
        path = f'{self.path}.pkl'
        with contextlib.suppress(Exception):
            with open(path, 'wb') as f:
                pickle.dump(self.url_pool, f)
            print('saved cache!')

    def close(self):
        self._dump_cache()
        self._dump_hubs()

    def __del__(self):
        """退出时自动调用写入缓存"""
        self.close()

    def _load_cache(self):
        """读取上次未完成抓取网址的数据"""
        path = f'{self.path}.pkl'
        with contextlib.suppress(Exception):
            with open(path, 'rb') as f:
                self.url_pool = pickle.load(f)
                print(f'load_cache:{self.url_pool}')

    def _load_hubs(self):
        with open(f'{self.path}_hubs.yml', 'r', encoding='utf-8') as f:
            self.hubs = yaml.load(f, Loader=yaml.CLoader)['hubs']
            print(f'loading hubs:{self.hubs}')
        return True

    def add(self, url, mode="url"):
        """将链接添加到网址池"""
        # 简单判断主机地址是否合法
        host = urlparse.urlparse(url).netloc  # 解析到主机地址
        if not host or '.' not in host:  # 简单判断主机地址是否合法
            print('地址错误:', url, ', len of ur:', len(url))
            return False
        issaved = self.db.has(url)
        if mode == 'url' and issaved:  # 是否存在数据库中
            return False

        # 遍历寻找url是否存在pool中
        for d in self.url_pool:
            if d.get('url', 0) == url:  # 取出字典,FIXME:此步骤没必要，待删除
                if time.time() - d["pendedtime"] > self.cfg[
                        'pending_threshold']:  # 判断是否超过刷新时间
                    d["pendedtime"] = 0
                    d["status"] = 'waiting'
                    return True
                return
        # url不存在pool中则：添加到pool中
        self.url_pool.append({
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
            self.add(urls, mode)
        else:
            for url in urls:
                self.add(url, mode)  # 遍历将url添加到网址池
                if mode == 'hub' and url not in self.hubs:
                    self.hubs.append(url)
                    self._dump_hubs()
                    self._load_hubs()

    def set_hubs(self, urls):
        """设置首页中的链接到网址池"""
        # 添加到网址池
        self.addmany(urls, mode="hub")

    def pop(self,
            count,
            hub_refresh_span=10,
            pending_threshold=10,
            hub_percent=50,
            limit=True
            ):  # sourcery skip: class-extract-method, extract-duplicate-method
        """
        弹出链接进入下载
        count:需要弹出的链接个数（并发）
        hub_percent:弹出的链接中hub_url的占比
        hub_refresh_span:hub链接刷新时间
        pending_threshold:url链接刷新时间
        limit:是否不允许弹出相同子域名
        return:[(hub,1),(url,0)]
        """
        hubs = []
        urls = []
        hosts = []
        hubs_count = count * hub_percent // 100
        urls_count = count - hubs_count
        for d in self.url_pool:
            if limit and d["host"] in hosts:
                continue
            _refresh = [
                time.time() - d['pendedtime'] > hub_refresh_span,
                time.time() - d['pendedtime'] > pending_threshold
            ]

            _count = [len(hubs) < hubs_count, len(urls) < urls_count]
            _index = self.url_pool.index(d)
            if d['mode'] == 'hub' and _refresh[0] and _count[0]:
                hubs.append((d['url'], 1))
                d['pendedtime'] = time.time()
                self.url_pool[_index]['status'] = 'pending'
            elif d['mode'] == 'url' and _refresh[1] and _count[1]:
                urls.append((d['url'], 0))
                d['pendedtime'] = time.time()
                self.url_pool[_index]['status'] = 'pending'
            if not _count[0] and not _count[1]:
                break
        return hubs + urls

    def set_status(self, url, status_code):
        """访问链接后设置状态"""
        # 简单判断主机地址是否合法
        host = urlparse.urlparse(url).netloc  # 解析到主机地址
        if not host or '.' not in host:
            print('地址错误:', url, ', len of ur:', len(url))
            return False
        _status = [d for d in self.url_pool if d['url'] == url]  # url是否存在pool中
        # _status = any(d for d in self.url_pool if d['url'] == url)
        if not _status:
            return False
        _status = _status[0]  # 去掉列表外壳：取到字典数据
        _index = self.url_pool.index(_status)  # 取出链接在pool中的索引

        # 根据链接状态保存到服务器
        mode = _status["mode"]
        if status_code == 200:
            if mode == 'url':
                self.url_pool.pop(_index)  # 下载完成删除url
            self.db.set_success(url)  # 写入到成功的网址池
            return True
        elif status_code == 404:
            self.db.set_failure(url)  # （地址不存在）写入到失败的网址池
            self.url_pool.pop(_index)
            return True

        # 访问失败时的处理
        if _status['failure'] > 0:  # 是否失败过
            self.url_pool[_index]['failure'] += 1  # 记录失败次数+1
            if mode == 'url' and _status['failure'] > self.cfg[
                    'failure_threshold']:  # 判断失败次数是否超过上限
                self.db.set_failure(url)  # 从数据库中设置失败状态
                self.url_pool.pop(_index)
            else:  # 没有达到失败上限
                self.url_pool[_index]['status'] = 'waiting'
        else:  # 第一次失败
            self.url_pool[_index]['failure'] = 1
            self.url_pool[_index]['status'] = 'waiting'

        return True