#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_quick_inbound.0.2.py
#CREATE_TIME: 2022-08-09
#AUTHOR: Sancho
"""
新闻爬虫
抛弃网址池，将所需要更新的数据整理后上传到数据库
效率:40页/分钟
"""

import lzma
import sys
import time
import yaml
import contextlib
import pymongo
from pymongo.mongo_client import MongoClient
import urllib.parse as urlparse
import requests
import re


class Loader:
    def __init__(self) -> None:
        my_dir = sys.path[0]
        my_name = my_dir.split('\\')[-1]
        self.path = f'{sys.path[0]}\\{my_name}'

    def load_conf(self):
        with open(f'{self.path}.yml', 'r', encoding='utf-8') as f:
            conf = yaml.load(f, Loader=yaml.CLoader)
        return conf

    def _dump_hubs(self, hubs):
        with contextlib.suppress(Exception):
            with open(f'{self.path}_hubs.yml', 'w') as f:
                yaml.dump(hubs, f)

    def load_hubs(self):
        with open(f'{self.path}_hubs.yml', 'r', encoding='utf-8') as f:
            hubs = yaml.load(f, Loader=yaml.CLoader)
            print(f'loading hubs:{hubs}')
        return hubs

    def re_load_conf(self, last_loading_time, refresh_time=300):
        if time.time() - last_loading_time > refresh_time:  # 每隔一段时间读取配置信息
            conf = self.load_conf()  # 读取配置文件
            hubs = self.load_hubs()  # 读取链接列表
            return time.time(), conf, hubs
        return last_loading_time, None, None


class Mongo:
    STATUS_FAILURE = b'0'
    STATUS_SUCCESS = b'1'
    sample = {
        'url': None,
        'host': None,
        'mode': None,
        'status': 'waiting',
        'pendedtime': 0,
        'failure': 0,
        'html': ''
    }

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

    def has(self, query):
        return self.coll.find_one(query)  # {'url': url}

    def insert(self, query, data):
        # 判断是否存在数据库
        if self.has(query):
            return False
        self.coll.insert_one(data)
        return True

    def insert_many(self, data):
        if not data:
            return False
        self.coll.insert_many(data)
        return True

    def update(self, query, data, upsert=False):
        self.coll.update_one(query, data, upsert)
        return True

    def update_many(self, filter, query, upsert=False):
        self.coll.update_many(filter, query, upsert)
        return True

    def get(self, count, refresh_time):
        isrefresh = time.time() - refresh_time
        return self.coll.find(
            {
                'pendedtime': {
                    '$lte': isrefresh
                },
                'status': {
                    '$ne': 'success'
                },
                'status': {
                    '$ne': 'failure'
                },
                'failure': {
                    '$lt': 3
                }
            },
            limit=count)


class Downloader:
    UA = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }

    def __init__(self) -> None:
        pass

    def fetch(self, session, url, headers=None, timeout=9):
        # TODO:UA池
        _headers = headers or self.UA
        try:
            resp = session.get(url, headers=_headers, timeout=timeout)
            status = resp.status_code
            resp.encoding = "utf-8"
            html = resp.text
            redirected_url = resp.url
            # print(f'succes download:{url} | status:{status}')
        except Exception as e:
            msg = f'Failed download: {url} | exception: {str(type(e))}, {str(e)}'
            print(msg)
            html = ''
            status = 0
            redirected_url = url
        return status, html, redirected_url


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

    def _get_hosts(self, urls):
        if isinstance(urls, str):
            return urlparse.urlparse(urls).netloc
        return [urlparse.urlparse(url).netloc for url in urls]

    def _zip_html(self, html):
        if not html:
            return ''
        if isinstance(html, str):
            html = html.encode('utf8')
        return lzma.compress(html)

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

    def process(self, status, html, redirected_url, mode, host):
        # 提取hub网页中的链接
        if status != 200:
            return False
        if mode == 'hub':
            newlinks = self._extract_links_re(redirected_url, html)
            goodlinks = self._filter_good(newlinks, host)
            print(f"{len(goodlinks)}/{len(newlinks)}, goodlinks/newlinks")
            # return goodlinks
            # REVIEW:
            return goodlinks


class Crawler:
    MAX_WORKERS_PROCESS, MAX_WORKERS_THREAD, MAX_WORKERS_CONCURRENT = 6, 12, 24

    def __init__(self) -> None:
        # 读取配置文件
        self.loader = Loader()
        self.conf = self.loader.load_conf()
        # 连接数据库
        self.mongo = Mongo(self.conf)
        # 其它功能
        self.downloader = Downloader()
        self.parser = Parser()

    def close(self):
        # 关闭数据库
        self.mongo.client.close()
        # 程序退出
        sys.exit()

    def __del__(self):
        self.close()

    def _ischange(self, conf, hubs):
        if not conf and not hubs:
            return False
        if conf == self.conf:
            self.conf = conf
        if self.conf['exit']:  #退出检测
            self.close()
        if difference := set(hubs).difference(set(self.hubs)):  # 添加到数据库
            self._save_to_db(difference, 'hub')
            self.hubs = hubs
        return True

    def _save_to_db(self, urls, mode):
        if not urls:
            return False
        documents = []
        for url in urls:
            sample = self.mongo.sample.copy()
            sample['mode'] = mode
            sample['url'] = url
            sample['host'] = self.parser._get_hosts(url)
            if not self.mongo.has({'url': url}):
                documents.append(sample)
        self.mongo.insert_many(documents)

        print(f'add links {len(documents)}')
        return True

    def _count_failure(self, url, failure_count, status):
        if status == 200:
            return failure_count
        if failure_count:
            if failure_count.get(url, None):
                failure_count[url] += 1
                return failure_count
            failure_count[url] = 1
            return failure_count
        failure_count[url] = 0
        return failure_count

    def crawl(self):
        failure_count = {}
        # 初始化链接
        last_loading_time = time.time()
        self.hubs = self.loader.load_hubs()
        self._save_to_db(self.hubs, 'hub')
        # 开启任务
        while 1:
            data = {}
            html_dict = {}
            links = []
            links_dict = {}

            print(f'========== {time.time()-last_loading_time} ==========')
            last_loading_time, conf, hubs = self.loader.re_load_conf(
                last_loading_time, 300)  # 刷新配置文件
            self._ischange(conf, hubs)  # 判断是否需要刷新
            tasks = self.mongo.get(self.MAX_WORKERS_CONCURRENT,
                                   self.conf['pending_threshold'])  # 获取链接

            with requests.session() as session:
                for task in tasks:  # 遍历待下载网址
                    # 抓取网页
                    # NOTE: 928  511886157.0 551601.5     79.2
                    status, html, redirected_url = self.downloader.fetch(
                        session, task['url'])

                    # 整理数据
                    failure_count = self._count_failure(
                        redirected_url, failure_count, status)
                    if failure_count:
                        failure = failure_count.get(redirected_url, 0)
                    else:
                        failure = 0
                    if task['mode'] == 'hub':
                        status_db = 'waiting'
                    elif failure <= 3:
                        status_db = 'sucess'
                    else:
                        status_db = 'failure'
                    pendedtime = time.time()
                    # NOTE: 464   96354962.0 207661.6     14.9
                    html_zip = self.parser._zip_html(html)

                    data[redirected_url] = {
                        'url': redirected_url,
                        'host': task['host'],
                        'mode': task['mode'],
                        'status': status_db,
                        'failure': failure,
                        'pendedtime': pendedtime,
                        'html': html_zip
                    }

                    if task['mode'] == 'hub':
                        html_dict[redirected_url] = {
                            'status': status,
                            'html': html,
                            'url': redirected_url,
                            'mode': task['mode'],
                            'host': task['host']
                        }

                # 解析数据
                for v in html_dict.values():
                    links.extend(
                        self.parser.process(v['status'], v['html'], v['url'],
                                            v['mode'], v['host']))

                # 格式化数据
                # NOTE: 40    1017762.0  25444.0      0.2
                links_dict = {
                    url: {
                        'host': self.parser._get_hosts(url),
                        'mode': 'url',
                        'status': 'waiting',
                        'failure': 0,
                        'pendedtime': 0,
                        'html': ''
                    }
                    for url in links
                }
                data.update(links_dict)

                # 更新状态
                # NOTE: 40   26121554.0 653038.8      4.0
                [
                    self.mongo.update({'url': url}, {'$set': v}, True)
                    for url, v in data.items()
                ]

    def run(self):
        try:
            self.crawl()
        except KeyboardInterrupt:
            print('stopped by yourself!')
        self.close()


if __name__ == '__main__':
    news_crawler = Crawler()
    news_crawler.run()