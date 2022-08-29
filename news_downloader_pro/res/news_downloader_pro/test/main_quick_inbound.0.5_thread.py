#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_quick_inbound.0.5_thread.py
#CREATE_TIME: 2022-08-15
#AUTHOR: Sancho
"""
新闻爬虫
多线程下载网页，解析后批量写入数据库
效率:1300页/分钟
修复：解析时只选择当前hub的子域名下的链接（将解析全部hub子域名下的链接）
"""

from concurrent.futures import ThreadPoolExecutor
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

        self._load_conf()
        self._load_hubs()

    def _load_conf(self):
        with open(f'{self.path}.yml', 'r', encoding='utf-8') as f:
            self.conf = yaml.load(f, Loader=yaml.CLoader)
        return self.conf

    def _load_hubs(self):
        with open(f'{self.path}_hubs.yml', 'r', encoding='utf-8') as f:
            self.hubs = yaml.load(f, Loader=yaml.CLoader)
            self.hubs = list(set(self.hubs))
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

    def _close(self):
        # self.client.close()
        pass

    def __del__(self):
        self._close()

    def get_url_list(self):
        urls = self.coll.find({}, {'_id': 0, 'url': 1})
        url_list = list(urls)
        return [url['url'] for url in url_list]

    def get_url_batch(self, query: dict, batch):
        return self.coll.find(query, {'html': 0}, limit=batch)

    def insert(self, query):
        self.coll.insert_many(query)

    def update(self, query):
        self.coll.update_one(query[0], query[1], upsert=True)


class Downloader:
    UA = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }

    def __init__(self) -> None:
        self.session = requests.session()

    def _close(self):
        # self.session.close()
        pass

    def __del__(self):
        self._close()

    def fetch(self, url, headers=None, timeout=9):
        # TODO:UA池
        if isinstance(url, dict):
            url = url['url']
        _headers = headers or self.UA
        try:
            resp = self.session.get(url, headers=_headers, timeout=timeout)
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


class Crawl:
    MAX_WORKERS_PROCESS, MAX_WORKERS_THREAD, MAX_WORKERS_CONCURRENT = 6, 12, 24

    def __init__(self) -> None:
        # 读取配置信息
        self.loader = Loader()
        self.conf = self.loader.conf
        self.hubs = self.loader.hubs
        # 链接数据库
        self.mongo = Mongo(self.conf)
        self.url_pool = self.mongo.get_url_list()  # 获取数据库内链接
        # 其它模块
        self.parser = Parser()
        self.downloader = Downloader()

    def _close(self):
        self.mongo._close()
        sys.exit()

    def __del__(self):
        self._close()

    def _refresh_files(self, last_loading_time, refreshtime=300):
        last_loading_time, conf, hubs_new = self.loader.re_load_conf(
            last_loading_time, refreshtime)
        if not conf and not hubs_new:
            return last_loading_time
        if conf != self.conf:
            self.conf = conf
        if time.time() - last_loading_time > refreshtime and self.conf[
                'exit'] == True:  #退出检测
            self._close()
        if difference := set(hubs_new).difference(set(self.hubs)):  # 添加到数据库
            hub_hosts = self.parser.get_hosts(difference)
            self.hub_hosts.append(hub_hosts)
            self.insert_urls_defult(difference, hub_hosts, 'hub')
        return time.time()

    def insert_urls_defult(self, urls, hosts, mode):
        urls = [url for url in urls if url not in self.url_pool]
        if not urls:
            return False
        urls = zip(urls, hosts)
        query = [{
            'url': url,
            'host': host,
            'mode': mode,
            'status': 'waiting',
            'pendedtime': 1,
            'failure': 0,
            'html': ''
        } for url, host in urls]
        self.mongo.insert(query)
        return True

    def get_waiting_urls_list(self):
        pending_threshold = time.time() - self.conf['pending_threshold']
        failure_threshold = self.conf['failure_threshold']
        return list(
            self.mongo.get_url_batch(
                {
                    'pendedtime': {
                        '$lt': pending_threshold
                    },
                    'status': 'waiting',
                    'failure': {
                        '$lt': failure_threshold
                    }
                }, self.MAX_WORKERS_CONCURRENT))

    def count_failure(self, url):
        if url in self.failure.keys():
            self.failure['url'] += 1
        else:
            self.failure['url'] = 1
        return True

    def crawler(self, tasks):
        with ThreadPoolExecutor(self.MAX_WORKERS_THREAD) as self.thread_pool1:
            features = self.thread_pool1.map(self.downloader.fetch, tasks)
        return features

    def _parser(self, status_code, html, url):
        links = []
        if link := self.parser.extract_links(status_code, html, url, 'hub',
                                             self.hub_hosts):
            links.extend(link)
        document = [[{'url': url}, {'$set': {'pendedtime': time.time()}}]]
        return links, document

    def _zip(self, html, url):
        status = 'success'
        failures = self.failure.get(url, 0)
        if failures >= 3:
            status = 'failure'
        html_zip = self.parser.zip_html(html, 'url')
        return [[{
            'url': url
        }, {
            '$set': {
                'status': status,
                'pendedtime': time.time(),
                'failure': failures,
                'html': html_zip
            }
        }]]

    def _append_links(self, links):
        links_hosts = self.parser.get_hosts(links)
        urls = zip(links, links_hosts)
        return [[{
            'url': url
        }, {
            '$set': {
                'url': url,
                'host': host,
                'mode': 'url',
                'status': 'waiting',
                'pendedtime': 2,
                'failure': 0,
                'html': ''
            }
        }] for url, host in urls if url not in self.url_pool]

    def _update(self, documents):
        with ThreadPoolExecutor(self.MAX_WORKERS_THREAD) as self.thread_pool2:
            self.thread_pool2.map(self.mongo.update, documents)
        return True

    def crawl_loop(self, tasks):
        # 下载网页
        features = self.crawler(tasks)

        documents = []
        links = []
        for status_code, html, url, _ in features:
            if status_code != 200:
                self.count_failure(url)
            # 解析数据
            if url in self.hubs:
                link, document = self._parser(status_code, html, url)
                links.extend(link)
                documents.extend(document)
                continue
            # 压缩数据
            documents.extend(self._zip(html, url))
        # 添加新链接
        if links:
            documents.extend(self._append_links(links))
        # 上传数据库
        self._update(documents)

    def crawl(self):
        # 将hubs存入数据库
        self.hub_hosts = self.parser.get_hosts(self.hubs)
        self.insert_urls_defult(self.hubs, self.hub_hosts, 'hub')
        # 时间记录
        last_loading_time = time.time()
        # 记录失败url和次数
        self.failure = {}

        # 循环进行下载、解析、入库
        while 1:
            print(f"---------- {time.time() - last_loading_time} ----------")
            # 到时间重新载入配置文件
            last_loading_time = self._refresh_files(last_loading_time, 2 * 60)
            # 获取待下载链接
            self.url_pool = self.mongo.get_url_list()
            url_list = self.get_waiting_urls_list()
            self.crawl_loop(url_list)

    def run(self):
        try:
            self.crawl()
        except KeyboardInterrupt:
            print('stopped by yourself!')
        self._close()


if __name__ == "__main__":
    crawler = Crawl()
    crawler.run()