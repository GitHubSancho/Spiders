#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_thread.py
#CREATE_TIME: 2022-08-17
#AUTHOR: Sancho
"""
新闻爬虫
多进程+多线程下载网页，并存储到数据库
效率:1300页/分钟
修复：解析时只选择当前hub的子域名下的链接（将解析全部hub子域名下的链接）
"""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, process
import lzma
from multiprocessing import Queue, pool
import multiprocessing
import re
import sys
import time
import urllib.parse as urlparse
import pymongo
import requests
import yaml
from pymongo.mongo_client import MongoClient


class Loader:
    def __init__(self) -> None:
        my_dir = sys.path[0]
        my_name = my_dir.split('\\')[-1]
        self.path = f'{sys.path[0]}\\{my_name}'

        self._load_conf()
        self._load_hubs()

    def _load_conf(self):
        with open(f'{self.path}.yml', 'r', encoding='utf-8') as f:
            self.conf = yaml.load(f, Loader=yaml.CLoader)
        return self.conf

    def _load_hubs(self):
        with open(f'{self.path}_hubs.yml', 'r', encoding='utf-8') as f:
            self.hubs = yaml.load(f, Loader=yaml.CLoader)
        return self.hubs

    def re_load_conf(self, last_loading_time, refresh_time=300):
        if time.time() - last_loading_time > refresh_time:  # 每隔一段时间读取配置信息
            conf = self._load_conf()  # 读取配置文件
            hubs = self._load_hubs()  # 读取链接列表
            return last_loading_time, conf, hubs
        return last_loading_time, None, None


class Mongo:
    def __init__(self, conf) -> None:
        self.user = conf['user']
        self.password = conf['password']
        self.host = conf['host']
        self.port = conf['port']
        self.database = conf['database']
        self.collection = conf['collection']
        self._client()

    def _client(self):
        if self.user and self.password:
            self.client = MongoClient(
                f'mongodb://{self.user}:{self.password}@{self.host}:{self.port}'
            )
        else:
            self.client = MongoClient(f'mongodb://{self.host}:{self.port}')

        self.db = self.client[self.database]
        self.coll = self.db[self.collection]
        self.coll.create_index([('url', pymongo.ASCENDING)],
                               unique=True)  # 创建索引
        return True

    def update(self, filter, update):
        self.coll.update_one(filter, update, True)

    def find(self, filter, projection=None):
        if projection:
            return self.coll.find(filter, projection)
        return self.coll.find(filter)


class Parser:
    G_BIN_POSTFIX = ('exe', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'pdf',
                     'jpg', 'png', 'bmp', 'jpeg', 'gif', 'zip', 'rar', 'tar',
                     'bz2', '7z', 'gz', 'flv', 'mp4', 'avi', 'wmv', 'mkv',
                     'apk')
    G_NEWS_POSTFIX = ('.html?', '.htm?', '.shtml?', '.shtm?')
    G_PATTERN_TAG_A = re.compile(
        r'<a[^>]*?href=[\'"]?([^> \'"]+)[^>]*?>(.*?)</a>', re.I | re.S | re.M)

    def __init__(self) -> None:
        pass

    def _clean_url(self, url):
        # 1. 是否为合法的http url
        if not url.startswith('http'):
            return ''
        # 2. 去掉静态化url后面的参数
        for np in self.G_NEWS_POSTFIX:
            p = url.find(np)
            if p > -1:
                p = url.find('?')
                url = url[:p]
                return url
        # 3. 不下载二进制类内容的链接
        up = urlparse.urlparse(url)
        path = up.path
        if not path:
            path = '/'
        postfix = path.split('.')[-1].lower()
        if postfix in self.G_BIN_POSTFIX:
            return ''
        # 4. 去掉标识流量来源的参数
        # badquery = ['spm', 'utm_source', 'utm_source', 'utm_medium', 'utm_campaign']
        good_queries = []
        for query in up.query.split('&'):
            qv = query.split('=')
            if qv[0].startswith('spm') or qv[0].startswith('utm_'):
                continue
            if len(qv) == 1:
                continue
            good_queries.append(query)
        query = '&'.join(good_queries)
        url = urlparse.urlunparse((
            up.scheme,
            up.netloc,
            path,
            up.params,
            query,
            ''  #  crawler do not care fragment
        ))
        return url

    def _filter_good(self, urls, hosts):
        goodlinks = []
        for url in urls:
            host = urlparse.urlparse(url).netloc
            if host in hosts:
                goodlinks.append(url)
        return goodlinks

    def _extract_links_re(self, url, html):
        """使用re模块从hub页面提取链接"""
        newlinks = set()
        aa = self.G_PATTERN_TAG_A.findall(html)
        for a in aa:
            link = a[0].strip()
            if not link:
                continue
            link = urlparse.urljoin(url, link)
            if link := self._clean_url(link):
                newlinks.add(link)
        # print("add:%d urls" % len(newlinks))
        return newlinks

    def extract_links(self, status, html, redirected_url, mode, host):
        # 提取hub网页中的链接
        if status != 200:
            return False
        if mode == 'hub':
            newlinks = self._extract_links_re(redirected_url, html)
            return self._filter_good(newlinks, host)

    def get_hosts(self, urls):
        if isinstance(urls, str):
            return urlparse.urlparse(urls).netloc
        return [urlparse.urlparse(url).netloc for url in urls]

    def zip_html(self, html, mode):
        if not html:
            return ''
        if mode == 'hub':
            return ''
        if isinstance(html, str):
            html = html.encode('utf8')
        return lzma.compress(html)


def set_hubs(hubs):
    hosts = parser.get_hosts(hubs)
    data = zip(hubs, hosts)
    data = [[{
        'url': url
    }, {
        '$set': {
            'url': url,
            'host': host,
            'mode': 'hub',
            'status': 'waiting',
            'pendedtime': 1,
            'failure': 0,
            'html': ''
        }
    }] for url, host in data]
    [mongo.update(filter, update) for filter, update in data]


def get_all_urls():
    if urls := mongo.find({}, {'url': 1, "_id": 0}):
        return list(urls)
    return urls


if __name__ == "__main__":
    # 读取文件配置
    loader = Loader()
    conf = loader.conf
    # 连接数据库
    mongo = Mongo(conf)
    # 初始化解释器
    parser = Parser()
    # 初始化链接
    hubs = loader.hubs
    set_hubs(hubs)
    # 获取链接
    url_pool = get_all_urls()
    print(url_pool)
