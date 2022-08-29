#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_concurrent.0.9.py
#CREATE_TIME: 2022-08-23
#AUTHOR: Sancho
"""
新闻爬虫
异步下载网页，并存储到数据库
优化：批量解析网页
优化：批量压缩网页
优化：异步操作数据库
优化：增加并发量
重构：链接提取
添加：访问失败记录
添加：UA池
添加：IP池（效率非常低）
"""

import random
import re
from secrets import choice
import sys
import lzma
import time
import yaml
import psutil
import pymongo
import aiohttp
import asyncio
import cchardet
import requests
from lxml import etree
import urllib.parse as urlparse
from concurrent.futures import ProcessPoolExecutor
from motor.motor_asyncio import AsyncIOMotorClient


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

    def load_ua(self):
        with open(f'{self.path}_ua.yml', 'r', encoding='utf-8') as f:
            ua_list = yaml.load(f, Loader=yaml.CLoader)
        return ua_list


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

    async def __len__(self, _filter=None):
        if not _filter:
            _filter = {}
        return await self.coll.count_documents(_filter)

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

        if html_zip:
            iter_zip = zip(urls, html_zip)
            [
                documents.extend(
                    Mongo._build_update_document(mode, url, status, failure,
                                                 html))
                for url, html in iter_zip
            ]
            return documents

        [
            documents.extend(
                Mongo._build_update_document(mode, url, status, failure,
                                             html_zip)) for url in urls
        ]
        return documents

    @staticmethod
    def _build_failure_documents(failures):
        if not failures:
            return []
        return [[{
            'url': url
        }, {
            '$set': {
                'failure': failure
            }
        }] for url, failure in failures.items()]

    def _client(self):
        self.client = AsyncIOMotorClient(self._build_uri(
            self.host, self.port, self.user, self.password),
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

    async def get(self, myfilter, projection=None, limit=0):
        if projection:
            documents = self.coll.find(myfilter, projection, limit=limit)
            return await documents.to_list(length=None)
        documents = self.coll.find(myfilter, limit=limit)
        return await documents.to_list(length=None)

    async def get_all_list(self):
        data = list(await self.get({}, {'url': 1, "_id": 0}))
        return [url['url'] for url in data]

    async def get_waiting_urls(self, pending_threshold, failure_threshold,
                               limit):
        refresh_time = time.time() - pending_threshold
        failure_threshold = failure_threshold
        return list(await self.get(
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

    async def update_one(self, document):
        try:
            await self.coll.update_one(document[0], document[1], upsert=True)
        except Exception as e:
            print(e)

    async def update_many(self, documents):
        [await self.update_one(document) for document in documents]

    async def len_downloaded_documents(self):
        return await self.__len__({'html': {"$ne": ""}})


class Downloader:
    def __init__(self, ua_list=None) -> None:
        if ua_list:
            self.ua_list = ua_list

    async def fetch(self, session, url, headers=None, proxy=None, timeout=3):
        html = ''
        status = 0
        redirected_url = url
        if isinstance(url, dict):
            url = url['url']
        _headers = headers or random.choice(self.ua_list)
        try:
            async with session.get(url,
                                   headers=_headers,
                                   proxy=proxy,
                                   verify_ssl=False,
                                   timeout=timeout) as resp:
                status = resp.status
                html = await resp.read()
                encoding = cchardet.detect(html)['encoding']
                html = html.decode(encoding, errors='ignore')
                redirected_url = resp.url
        except aiohttp.ClientConnectorError as e:
            pass
        except OSError as e:
            pass
        except Exception as e:
            msg = f'Failed download: {url} | exception: {str(type(e))}, {str(e)}'
            # print(msg)
        return status, html, url, redirected_url, proxy

    async def crawler(self, session, task, proxy=None):
        task = [
            asyncio.create_task(self.fetch(session, url['url'], proxy=proxy))
            for url in task
        ]
        done, _ = await asyncio.wait(task)

        return [l._result for l in done]

    async def test_proxy(self, session, task):
        task = [
            asyncio.create_task(
                self.fetch(session, TEST_URL, proxy=proxy, timeout=10))
            for proxy in task
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
        if not html:
            return
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
        return list(newlinks)


class Proxy:
    def __init__(self) -> None:
        pass

    def get_proxy_ip(self):
        resp = requests.get(PROXYPOOL_URL, params=PROXYPOOL_API_PARAMS)
        html = resp.text
        _ip = html.split('</br>')[:-1]
        return [f'http://{ip}' for ip in _ip]

    def build_proxy_pool(self, proxy):
        if not proxy:
            return []
        self.proxy_pool = {p: 50 for p in proxy}
        print(f'加载ip：{len(self.proxy_pool)}')
        return self.proxy_pool

    def sub_proxy_count(self, ip):
        count = self.proxy_pool.get(ip, None)
        if count is None:
            return None
        elif count <= 0:
            del self.proxy_pool[ip]
            return None
        self.proxy_pool[ip] -= 1
        return ip

    def choices(self, k):
        if not self.proxy_pool:
            return None
        keys = list(self.proxy_pool.keys())
        return random.choices(keys, k=k)


class Engine:
    def __init__(self) -> None:
        # 读取配置文件
        self.loader = Loader()
        self.conf, self.hubs = self.loader.reload_files()
        ua_list = self.loader.load_ua()
        # 连接数据库
        self.mongo = Mongo(self.conf)
        # 其它模块
        self.downloader = Downloader(ua_list)
        self.parser = Parser()
        # self.proxy = Proxy()
        # 初始化变量
        self.failure_pool = {}

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

    async def refresh_files(self, refreshtime=300):
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
            await self.mongo.update_many(documents)
            self.hubs = hubs_new

        return

    async def tab_url(self, features):
        htmls = []
        urls = {}
        hubs = []
        for status_code, html, url, _0, _1 in features:
            if status_code != 200:  # 标记访问错误
                # self._proxy = self.proxy.sub_proxy_count(_1)
                if self.failure_pool.get(url, False):
                    if self.failure_pool[url] >= 3:
                        del self.failure_pool[url]  # 访问失败三次从内存删除
                        continue
                    self.failure_pool[url] += 1  # 增加访问失败计数
                    continue
                self.failure_pool[url] = 1  # 添加访问失败计数
            if url in self.hubs:  # 拼接hub链接的html
                htmls.append((url, html))
                hubs.append(url)
                continue
            urls[url] = html  # 标记普通url html
        return htmls, hubs, urls

    def extract(self, htmls):
        links = process_pool.map(extract_links, htmls)
        if not links:
            return
        l = []
        [l.extend(link) for link in links if link]
        return self.parser._filter_good(l, self.hubs_hosts)

    def build_documents(self, hubs, urls, links):
        documents = []
        # 生成访问错误的文档
        if self.failure_pool:
            failure_documents = Mongo._build_failure_documents(
                self.failure_pool)
            documents.extend(failure_documents)
        # 生成hubs更新文档
        if hubs:
            hubs_documents = Mongo._build_update_documents('hub', hubs)
            documents.extend(hubs_documents)
        # 生成普通url文档
        if urls:
            _urls = urls.keys()
            _htmls = urls.values()
            _htmls_zip = process_pool.map(zip_html, _htmls)
            url_documents = Mongo._build_update_documents(
                'url', _urls, 'success', 0, _htmls_zip)
            documents.extend(url_documents)
        # 生成新url文档
        if links:
            links = [link for link in links if link not in self.urls]
            links_hosts = self.get_hosts(links)
            links_documents = Mongo._build_defult_documents(
                'url', zip(links, links_hosts))
            documents.extend(links_documents)
        return documents

    async def get_proxy(self):
        proxy = self.proxy.get_proxy_ip()
        result = await self.downloader.test_proxy(self.session, proxy)
        proxy = [t[4] for t in result if t if t[0] == 200]
        return self.proxy.build_proxy_pool(proxy)

    async def start(self):
        # 异步函数
        # 上传初始链接
        self.hubs_hosts, documents = self.pack_documents('hub', self.hubs)
        await self.mongo.update_many(documents)
        # 初始化变量
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.last_loading_time = time.time()
        self._proxy = None
        # assert await self.get_proxy()
        # self._proxy = self.proxy.choices(1)[0]
        print("- 开始抓取 -")
        while 1:
            # for _i in range(5):
            # 计时重读配置文件
            await self.refresh_files(60 * 2)
            # 获取待下载链接
            self.urls = await self.mongo.get_all_list()
            task = await self.mongo.get_waiting_urls(
                self.conf['pending_threshold'], self.conf['failure_threshold'],
                MAX_WORKERS_CONCURRENT)
            # 执行任务
            if not task:
                continue
            # if not self._proxy:
            #     await self.get_proxy()
            #     self._proxy = self.proxy.choices(1)[0]
            # 下载网页
            features = await self.downloader.crawler(self.session, task,
                                                     self._proxy)
            # 标记链接
            htmls, hubs, urls = await self.tab_url(features)
            # 解析网页
            # NOTE: 20   16863600.0 843180.0     31.9
            good_links = self.extract(htmls)
            # 生成文档
            # NOTE: 20   35603163.0 1780158.1     67.3
            documents = self.build_documents(hubs, urls, good_links)
            # 上传文档
            await self.mongo.update_many(documents)
            # 查看状态
            print(
                f'已下载页面/全部链接： {await self.mongo.len_downloaded_documents()}/{len(await self.mongo.get_all_list())}'
            )

    def run(self):
        try:
            # 协程模块
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(self.start())
        except KeyboardInterrupt:
            print('stopped by yourself!')
        self._close()


def extract_links(document):
    parser = Parser()
    return parser._extract_links_re(document[0], document[1])


def zip_html(html):
    if not html:
        return ''
    if isinstance(html, str):
        html = html.encode('utf8')
    return lzma.compress(html)


def check_memory():
    # 检查内存
    mem = psutil.virtual_memory()
    zj = float(mem.total) / 1000000000
    ysy = float(mem.used) / 1000000000
    return int((0.8 * zj - ysy) / 0.16 * 24)


if __name__ == "__main__":
    MAX_WORKERS_CONCURRENT = check_memory()
    # TEST_URL = 'https://www.baidu.com'
    # PROXYPOOL_URL = 'http://webapi.http.zhimacangku.com/getip'
    # PROXYPOOL_API_KEY = '262150'
    # PROXYPOOL_API_PARAMS = {
    #     'num': 100,
    #     'type': 3,
    #     'pro': 0,
    #     'city': 0,
    #     'yys': 0,
    #     'port': 1,
    #     'time': 1,
    #     # 'pack': PROXYPOOL_API_KEY,
    #     'ts': 0,
    #     'ys': 0,
    #     'cs': 0,
    #     'lb': 2,
    #     'sb': 0,
    #     'pb': 45,
    #     'mr': 1,
    #     'regions': ''
    # }
    with ProcessPoolExecutor() as process_pool:
        engine = Engine()
        engine.run()
