#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_quick_inbound.0.6_process.py
#CREATE_TIME: 2022-08-17
#AUTHOR: Sancho
"""
新闻爬虫
多进程+多线程下载网页，并存储到数据库
效率:2470页/分钟
"""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import lzma
from multiprocessing import Manager, Lock, Queue
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

    def load_hubs(self):
        with open(f'{self.path}_hubs.yml', 'r', encoding='utf-8') as f:
            hubs = yaml.load(f, Loader=yaml.CLoader)
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
        data = list(self.get(_filter, {'url': 1, '_id': 0}))
        return len([url['url'] for url in data])

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

    def _client(self):
        self.client = MongoClient(self._build_uri(self.host, self.port,
                                                  self.user, self.password),
                                  maxPoolSize=None)
        self.db = self.client[self.database]
        self.coll = self.db[self.collection]
        self.coll.create_index([('url', pymongo.ASCENDING)],
                               unique=True)  # 创建索引
        return True

    def _close(self):
        if self.client:
            self.client.close()

    def get(self, filter, projection=None, limit=0):
        if projection:
            return self.coll.find(filter, projection, limit=limit)
        return self.coll.find(filter, limit=limit)

    def get_all_list(self, projection=None):
        if projection is None:
            projection = {'_id': 0, 'url': 1}
        url_list = list(self.get({}, projection))
        return [url['url'] for url in url_list]

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

    def get_url_batch(self, query: dict, batch):
        return self.coll.find(query, {'html': 0}, limit=batch)

    def update(self, filter):
        try:
            self.coll.update_one(filter[0], filter[1], upsert=True)
        except Exception as e:
            print(e)

    def update_many(self, documents):
        [self.update(document) for document in documents]

    def update_documents(self, documents, workers):
        with ThreadPoolExecutor(workers) as thread_pool2:
            thread_pool2.map(self.update, documents)
        return True

    def get_task(self, pending_threshold, failure_threshold,
                 concuttent_workers, _):
        return self.get_waiting_urls(pending_threshold, failure_threshold,
                                     concuttent_workers)

    def get_tasks(self, pending_threshold, failure_threshold,
                  concuttent_workers, process_workers):
        tasks = self.get_waiting_urls(pending_threshold, failure_threshold,
                                      concuttent_workers * process_workers)
        return [
            tasks[i:i + concuttent_workers]
            for i in range(0, len(tasks), concuttent_workers)
        ]

    def get_downloaded_num(self):
        return self.__len__({'html': {"$ne": ""}})


class Downloader:
    UA = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }

    def __init__(self) -> None:
        pass

    @classmethod
    def fetch(cls, session, url, headers=None, timeout=9):
        # TODO:UA池
        if isinstance(url, dict):
            url = url['url']
        _headers = headers or Downloader.UA
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
        return status, html, url, redirected_url

    @staticmethod
    def crawler(session, task, workers):
        with ThreadPoolExecutor(workers) as thread_pool1:
            features = [
                thread_pool1.submit(Downloader().fetch, session, url).result()
                for url in task
            ]
        return features


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

    @staticmethod
    def get_hosts(urls):
        if isinstance(urls, str):
            return urlparse.urlparse(urls).netloc
        return [urlparse.urlparse(url).netloc for url in urls]

    @staticmethod
    def get_defult_documents(mode, links):
        links_hosts = Parser.get_hosts(links)
        documents = zip(links, links_hosts)
        return Mongo._build_defult_documents(mode, documents)

    @staticmethod
    def process(features, hubs):
        parser = Parser()
        documents = []
        links = []
        for status_code, html, url, _ in features:
            if status_code != 200:
                # TODO:访问失败处理
                # self.count_failure(url)
                continue
            # 解析数据
            if url in hubs:
                hubs_hosts = set(Parser.get_hosts(hubs))
                link, document = parser._parse_html(status_code, html, url,
                                                    hubs_hosts)
                links.extend(link)
                documents.extend(document)
                continue
            # 压缩数据
            documents.extend(parser._get_url_document(html, url))
        return documents, links

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

    def _parse_html(self, status_code, html, url, hosts):
        links = []
        links.extend(self.extract_links(status_code, html, url, 'hub', hosts))
        document = Mongo._build_update_document('hub', url)
        return links, document

    def _zip_html(self, html, mode):
        if not html:
            return ''
        if mode == 'hub':
            return ''
        if isinstance(html, str):
            html = html.encode('utf8')
        return lzma.compress(html)

    def _get_url_document(self, html, url):
        status = 'success'
        # TODO: 访问失败处理
        # failures = self.failure.get(url, 0)
        # if failures >= 3:
        #     status = 'failure'
        html_zip = self._zip_html(html, 'url')
        return Mongo._build_update_document('url', url, status, 0, html_zip)

    def extract_links(self, status, html, redirected_url, mode, hosts):
        # 提取hub网页中的链接
        if status != 200:
            return False
        if mode == 'hub':
            newlinks = self._extract_links_re(redirected_url, html)
            return self._filter_good(newlinks, hosts)


class Engine:
    def __init__(self) -> None:
        # 读取配置信息
        self.loader = Loader()
        self.conf = self.loader.load_conf()
        self.hubs = self.loader.load_hubs()
        # 链接数据库
        self.mongo = Mongo(self.conf)
        # 其它模块
        self.parser = Parser()
        self.downloader = Downloader()
        # 时间记录
        self.last_loading_time = time.time()
        # 记录失败url和次数
        self.failure = {}

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
            sys.exit()
        if difference := set(hubs_new).difference(set(self.hubs)):  # 添加到数据库
            documents = Parser.get_defult_documents('hub', difference)
            [self.mongo.update(document) for document in documents]
            self.hubs = hubs_new
        return


def crawl_loop(task, conf, hubs, url_pool, queue, lock):
    # 下载网页
    with requests.session() as session:
        features = Downloader.crawler(session, task, MAX_WORKERS_THREAD)
    # 解析和压缩网页
    documents, links = Parser.process(features, hubs)
    # 添加新链接
    if links:
        links = set(links).difference(url_pool)
        documents.extend(Parser.get_defult_documents('url', links))
    # 上传数据
    mongo = Mongo(conf)
    mongo.update_documents(documents, MAX_WORKERS_THREAD)
    print(f"已有页面：{mongo.get_downloaded_num()}")


def crawl():
    # 读取配置
    engine = Engine()
    conf = engine.loader.load_conf()
    hubs = engine.loader.load_hubs()
    # 将hubs存入数据库
    documents = engine.parser.get_defult_documents('hub', hubs)
    engine.mongo.update_many(documents)
    process_manager = Manager()
    queue = process_manager.Queue(10)
    lock = process_manager.Lock()
    # warp = lp(crawl_loop)
    while 1:
        # for _ in range(10):
        # 到时间重新载入配置文件
        engine.refresh_files(60 * 2)
        conf = engine.conf
        hubs = engine.hubs
        # 获取待下载链接（多进程）
        engine.url_pool = engine.mongo.get_all_list()
        tasks = engine.mongo.get_tasks(conf['pending_threshold'],
                                       conf['failure_threshold'],
                                       MAX_WORKERS_CONCURRENT,
                                       MAX_WORKERS_PROCESS)

        with ProcessPoolExecutor(MAX_WORKERS_PROCESS) as process_pool:
            [
                process_pool.submit(crawl_loop, task, conf, hubs,
                                    engine.url_pool, queue, lock)
                for task in tasks
            ]
        # 获取待下载链接（单进程）
        # engine.url_pool = engine.mongo.get_all_list()
        # tasks = engine.mongo.get_task(conf['pending_threshold'],
        #                               conf['failure_threshold'],
        #                               MAX_WORKERS_CONCURRENT,
        #                               MAX_WORKERS_PROCESS)

        # crawl_loop(tasks, conf, hubs, engine.url_pool, queue, lock)
        # 打印时间
        print(f"---------- {time.time()-engine.last_loading_time} ----------")


MAX_WORKERS_PROCESS, MAX_WORKERS_THREAD, MAX_WORKERS_CONCURRENT = 6, 12, 24
if __name__ == "__main__":
    try:
        # lp = LineProfiler()
        crawl()
        # lp.print_stats()
    except KeyboardInterrupt:
        print('stopped by yourself!')
