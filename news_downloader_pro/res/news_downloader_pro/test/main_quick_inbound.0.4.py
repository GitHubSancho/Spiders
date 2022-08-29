#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_quick_inbound.0.4.py
#CREATE_TIME: 2022-08-14
#AUTHOR: Sancho
"""
新闻爬虫
抛弃网址池，将所需要更新的数据整理后上传到数据库
效率:220页/分钟
- 修复：读文件时没有倒计时
- 修复：多次查询时有重复项
"""

import contextlib
import lzma
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
            # print(f'loading hubs:{hubs}')
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
        'status': None,
        'pendedtime': None,
        'failure': None,
        'html': None
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

    def _get_list(self):
        return [
            l['url'] for l in list(self.coll.find({}, {
                "_id": 0,
                "url": 1
            }))
        ]

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

    def get(self, count, refresh_time, failure_threshold):
        isrefresh = time.time() - refresh_time
        return self.coll.find(
            {
                'pendedtime': {
                    '$lt': isrefresh
                },
                'status': 'waiting',
                'failure': {
                    '$lt': failure_threshold
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

    def _zip_html(self, html, mode):
        if not html:
            return ''
        if mode == 'hub':
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
            return self._filter_good(newlinks, host)


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

    def _formatting_sample(self, values, isup=False):
        documents = {}
        for v in values:
            sample = {
                'url': v[0],
                'host': v[1],
                'mode': v[2],
                'status': v[3],
                'pendedtime': v[4],
                'failure': v[5],
                'html': v[6]
            }
            if isup:
                documents[v[0]] = {'$set': sample}
                continue
            documents[v[0]] = sample
        return documents

    def _push_hubs(self, hubs=None):
        # 读取hubs文件
        if not hubs:
            hubs = self.loader.load_hubs()
        if not hubs:
            return
        # 格式化hub链接
        hubs_zip = zip(hubs, self.parser._get_hosts(hubs))
        data = [[hub, host, 'hub', 'waiting', 0, 0, '']
                for hub, host in hubs_zip]
        documents = self._formatting_sample(data, True)
        [
            self.mongo.update({url: 'url'}, document, True)
            for url, document in documents.items()
        ]
        return hubs

    def _refresh_files(self, last_loading_time, refreshtime=300):
        last_loading_time, conf, hubs_new = self.loader.re_load_conf(
            last_loading_time, refreshtime)
        if not conf and not hubs_new:
            return last_loading_time
        if conf != self.conf:
            self.conf = conf
        if self.conf['exit'] and last_loading_time > time.time(
        ) - refreshtime:  #退出检测
            self.close()
        if difference := set(hubs_new).difference(set(self.hubs)):  # 添加到数据库
            self.hubs, _ = self._push_hubs(difference)
        return time.time()

    def _count_failure(self, url, status):
        if status == 200:
            return 0
        if self.failures:
            if self.failures.get(url, None):
                self.failures[url] += 1
                return self.failures[url]
            self.failures[url] = 1
            return 1
        self.failures[url] = 0
        return 0

    def _arrange_document(self, url, host, mode, status_code, html, isup=True):
        failure = self._count_failure(url, status_code)
        if mode == 'hub':
            status = 'waiting'
        elif failure <= 3:
            status = 'sucess'
        else:
            status = 'failure'
        pendedtime = time.time()
        return self._formatting_sample(
            [[url, host, mode, status, pendedtime, failure, html]], isup)

    def _crawl_loop(self, task, documents):
        # def _crawl_loop(self, session, task, documents):
        print(
            f"{task['url']},{task['mode']},{task['status']},{task['pendedtime']}"
        )
        # 抓取网页
        status_code, html, redirected_url = self.downloader.fetch(
            self.session, task['url'])

        # 压缩数据
        html_zip = self.parser._zip_html(html, task['mode'])

        # 解析数据
        links = self.parser.process(status_code, html, task['url'],
                                    task['mode'], task['host'])

        # 整理数据
        documents.update(
            self._arrange_document(task['url'], task['host'], task['mode'],
                                   status_code, html_zip, True))

        if links:
            for link in links:
                if link not in self.url_db:
                    documents.update(
                        self._formatting_sample([[
                            link,
                            self.parser._get_hosts(link), 'url', 'waiting', 1,
                            0, ''
                        ]], True))
        return documents

    def _update_documents(self, documents):
        return [
            self.mongo.update({'url': url}, document, True)
            for url, document in documents.items()
        ]

    def crawl(self):
        # 初始化链接
        last_loading_time = time.time()
        self.hubs = self._push_hubs()
        self.failures = {}
        # 开启任务
        # REVIEW:

        while 1:
            # for _i in range(20):
            # 刷新配置文件
            print(f'========== {time.time()-last_loading_time} ==========')
            last_loading_time = self._refresh_files(last_loading_time, 5 * 60)
            # 获取链接
            tasks = self.mongo.get(self.MAX_WORKERS_CONCURRENT,
                                   self.conf['pending_threshold'],
                                   self.conf['failure_threshold'])
            self.url_db = self.mongo._get_list()
            # 遍历待下载链接
            documents = {}
            with requests.session() as self.session:
                for task in tasks:
                    documents = self._crawl_loop(task, documents)

                # 更新状态
                self._update_documents(documents)

    def run(self):
        try:
            # REVIEW:

            self.crawl()

            # lp.print_stats()
        except KeyboardInterrupt:
            print('stopped by yourself!')
        self.close()


if __name__ == '__main__':
    news_crawler = Crawler()
    # REVIEW:
    # from line_profiler import LineProfiler
    # lp = LineProfiler()

    news_crawler.run()
