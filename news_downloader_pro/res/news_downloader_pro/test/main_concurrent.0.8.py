#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_concurrent.0.8.py
#CREATE_TIME: 2022-08-23
#AUTHOR: Sancho
"""
新闻爬虫
重构：异步下载网页，并存储到数据库
"""

import lzma
import re
import sys
import time
import urllib.parse as urlparse
import pymongo
import yaml
from pymongo.mongo_client import MongoClient
import aiohttp
import asyncio
import cchardet


class Loader:
    def __init__(self) -> None:
        my_dir = sys.path[0]
        my_name = my_dir.split('\\')[-1]
        self.path = f'{sys.path[0]}\\{my_name}'

    def load_conf(self):
        with open(f'{self.path}.yml', 'r', encoding='utf-8') as f:
            conf = yaml.load(f, Loader=yaml.CLoader)
        return conf

    def load_hubs(self):
        with open(f'{self.path}_hubs.yml', 'r', encoding='utf-8') as f:
            hubs = yaml.load(f, Loader=yaml.CLoader)
            hubs = list(set(hubs))
        return hubs

    def reload_files(self):
        conf = self.load_conf()  # 读取配置文件
        hubs = self.load_hubs()  # 读取链接列表
        return conf, hubs


class Mongo:
    def __init__(self, conf) -> None:
        self.user = conf['user']
        self.password = conf['password']
        self.host = conf['host']
        self.port = conf['port']
        self.database = conf['database']
        self.collection = conf['collection']
        self._client()

    def __del__(self):
        self._close()

    def __len__(self, _filter=None):
        if not _filter:
            _filter = {}
        return self.coll.count_documents(_filter)

    @staticmethod
    def _build_uri(host, port, user=None, password=None):
        if user and password:
            return f'mongodb://{user}:{password}@{host}:{port}'
        return f'mongodb://{host}:{port}'

    @staticmethod
    def _build_defult_document(mode, url, host):
        return [{
            'url': url
        }, {
            '$set': {
                'url': url,
                'host': host,
                'mode': mode,
                'status': 'waiting',
                'pendedtime': 1,
                'failure': 0,
                'html': ''
            }
        }]

    @staticmethod
    def _build_defult_documents(mode, documents):
        return [
            Mongo._build_defult_document(mode, url, host)
            for url, host in documents
        ]

    @staticmethod
    def _build_update_document(mode,
                               url,
                               status=None,
                               failure=None,
                               html_zip=None):
        if mode == 'hub':
            return [[{'url': url}, {'$set': {'pendedtime': time.time()}}]]
        return [[{
            'url': url
        }, {
            '$set': {
                'status': status,
                'pendedtime': time.time(),
                'failure': failure,
                'html': html_zip
            }
        }]]

    @staticmethod
    def _build_update_documents(mode,
                                urls,
                                status=None,
                                failure=None,
                                html_zip=None):
        documents = []
        [
            documents.extend(
                Mongo._build_update_document(mode, url, status, failure,
                                             html_zip)) for url in urls
        ]
        return documents

    def _client(self):
        self.client = MongoClient(self._build_uri(self.host, self.port,
                                                  self.user, self.password),
                                  maxPoolSize=None)
        self._connect_db()
        return True

    def _connect_db(self):
        self.db = self.client[self.database]
        self.coll = self.db[self.collection]
        self.coll.create_index([('url', pymongo.ASCENDING)], unique=True)
        # 创建索引
        return

    def _close(self):
        if self.client:
            self.client.close()

    def get(self, myfilter, projection=None, limit=0):
        if projection:
            return self.coll.find(myfilter, projection, limit=limit)
        return self.coll.find(myfilter, limit=limit)

    def get_all_list(self):
        data = list(self.get({}, {'url': 1, "_id": 0}))
        return [url['url'] for url in data]

    def get_waiting_urls(self, pending_threshold, failure_threshold, limit):
        refresh_time = time.time() - pending_threshold
        failure_threshold = failure_threshold
        return list(
            self.get(
                {
                    'pendedtime': {
                        '$lt': refresh_time
                    },
                    'status': 'waiting',
                    'failure': {
                        '$lt': failure_threshold
                    }
                },
                limit=limit))

    def update_one(self, document):
        try:
            self.coll.update_one(document[0], document[1], upsert=True)
        except Exception as e:
            print(e)

    def update_many(self, documents):
        [self.update_one(document) for document in documents]

    def len_downloaded_documents(self):
        return self.__len__({'html': {"$ne": ""}})


class Downloader:
    UA = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }

    def __init__(self) -> None:
        pass

    async def fetch(self, session, url, headers=None):
        # TODO:UA池
        if isinstance(url, dict):
            url = url['url']
        _headers = headers or self.UA
        try:
            async with session.get(url,
                                   headers=_headers,
                                   verify_ssl=False,
                                   timeout=3) as resp:
                status = resp.status
                html = await resp.read()
                encoding = cchardet.detect(html)['encoding']
                html = html.decode(encoding, errors='ignore')
                redirected_url = resp.url
        except Exception as e:
            msg = f'Failed download: {url} | exception: {str(type(e))}, {str(e)}'
            print(msg)
            html = ''
            status = 0
            redirected_url = url
        return status, html, url, redirected_url

    async def crawler(self, session, task):
        task = [
            asyncio.create_task(self.fetch(session, url['url']))
            for url in task
        ]
        done, _ = await asyncio.wait(task)

        return [l._result for l in done]


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

    def _get_url_document(self, html, url):
        status = 'success'
        # TODO: 访问失败处理
        # failures = self.failure.get(url, 0)
        # if failures >= 3:
        #     status = 'failure'
        html_zip = self.zip_html(html, 'url')
        return Mongo._build_update_document('url', url, status, 0, html_zip)

    def zip_html(self, html, mode):
        if not html:
            return ''
        if mode == 'hub':
            return ''
        if isinstance(html, str):
            html = html.encode('utf8')
        return lzma.compress(html)

    def extract_links(self, status, html, redirected_url, mode, hosts):
        # 提取hub网页中的链接
        if status != 200:
            return False
        if mode == 'hub':
            newlinks = self._extract_links_re(redirected_url, html)
            return self._filter_good(newlinks, hosts)

    def parse_html(self, status_code, html, url, hosts):
        links = []
        links.extend(self.extract_links(status_code, html, url, 'hub', hosts))
        document = Mongo._build_update_document('hub', url)
        return links, document


class Engine:
    def __init__(self) -> None:
        # 读取配置文件
        self.loader = Loader()
        self.conf, self.hubs = self.loader.reload_files()
        # 连接数据库
        self.mongo = Mongo(self.conf)
        # 上传初始链接
        self.hubs_hosts, documents = self.pack_documents('hub', self.hubs)
        self.mongo.update_many(documents)
        # 其它模块
        self.downloader = Downloader()
        self.parser = Parser()

    def __del__(self):
        self._close()

    @staticmethod
    def get_hosts(urls):
        if isinstance(urls, str):
            return urlparse.urlparse(urls).netloc
        return [urlparse.urlparse(url).netloc for url in urls]

    @staticmethod
    def pack_documents(mode, urls):
        hosts = Engine.get_hosts(urls)
        documents = Mongo._build_defult_documents(mode, zip(urls, hosts))
        return hosts, documents

    def _close(self):
        sys.exit("done!")

    def refresh_files(self, refreshtime=300):
        if time.time() - self.last_loading_time < refreshtime:
            return
        self.last_loading_time = time.time()
        conf_new, hubs_new = self.loader.reload_files()
        if not conf_new and not hubs_new:
            return
        if self.conf != conf_new:
            self.conf = conf_new
        if self.conf['exit'] is True:  #退出检测
            self._close()
        if difference := set(hubs_new).difference(set(self.hubs)):  # 添加到数据库
            hubs_hosts, documents = self.pack_documents('hub', difference)
            self.hubs_hosts.extend(hubs_hosts)
            self.mongo.update_many(documents)
            self.hubs = hubs_new

        return

    def process(self, features):
        documents = []
        links = []
        for status_code, html, url, _ in features:
            if status_code != 200:
                # TODO:访问失败处理
                # self.count_failure(url)
                continue
            # 解析数据
            if url in self.hubs:
                # NOTE: 262   43443687.0 165815.6     30.0
                link, document = self.parser.parse_html(
                    status_code, html, url, self.hubs_hosts)
                links.extend(link)
                documents.extend(document)
                continue
            # 压缩数据
            # NOTE: 287  101241983.0 352759.5     70.0
            documents.extend(self.parser._get_url_document(html, url))
        return documents, links

    async def start(self):
        # 异步函数
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.last_loading_time = time.time()
        while 1:
            # for _ in range(20):
            # 计时重读配置文件
            self.refresh_files(60 * 2)
            # 获取待下载链接
            self.url_pool = self.mongo.get_all_list()
            task = self.mongo.get_waiting_urls(self.conf['pending_threshold'],
                                               self.conf['failure_threshold'],
                                               MAX_WORKERS_CONCURRENT)
            # 执行任务
            # 下载网页
            features = await self.downloader.crawler(self.session, task)
            # 解析和添加网页
            # NOTE: 20   55433387.0 2771669.4     92.0
            documents, links = self.process(features)
            # 添加新链接
            if links:
                links = set(links).difference(self.url_pool)
                _, document = self.pack_documents('url', links)
                documents.extend(document)
            # 更新数据库
            # NOTE: 20    3169161.0 158458.0      5.3
            self.mongo.update_many(documents)
            print(
                f"已有页面：{self.mongo.len_downloaded_documents()}/已有链接：{len(self.mongo)}"
            )

    def run(self):
        try:
            # 协程模块
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(self.start())

        except KeyboardInterrupt:
            print('stopped by yourself!')
        self._close()


MAX_WORKERS_PROCESS, MAX_WORKERS_THREAD, MAX_WORKERS_CONCURRENT = 6, 12, 24
if __name__ == "__main__":
    engine = Engine()
    engine.run()