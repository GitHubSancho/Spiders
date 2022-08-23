#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_quick_inbound.0.1.py
#CREATE_TIME: 2022-08-09
#AUTHOR: Sancho
"""
新闻爬虫
抛弃网址池，直接使用数据库查询和存储
效率:30页/分钟
"""

import lzma
import sys
import time
import yaml
import contextlib
import pickle
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
        'failure': 0
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
        return True

    def _set_timenow(self, url):
        self.update({'url': url}, {'$set': {'pendedtime': time.time()}})
        return True

    def _set_success(self, url, html):
        self.update({'url': url}, {
            '$set': {
                'status': 'success',
                'pendedtime': time.time(),
                'html': html
            }
        })

    def _set_failure(self, url):
        self.update({'url': url},
                    {'$set': {
                        'status': 'failure',
                        'pendedtime': time.time()
                    }})
        return True

    def _set_failure_again(self, url, failure_count):
        self.update({'url': url}, {
            '$set': {
                'failure': failure_count + 1,
                'pendedtime': time.time()
            }
        })
        return True

    def _set_status(self, status, html, url, mode, failure_count,
                    failure_threshold):
        if mode == 'hub':
            self._set_timenow(url)
            return True
        if status == 200:
            self._set_success(url, html)
            return True
        elif status == 404 or failure_count >= failure_threshold - 1:
            self._set_failure(url)
            return False
        self._set_failure_again(url, failure_count)
        return False

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

    def update(self, query, data):
        self.coll.update_one(query, data)
        return True

    def update_many(self, filter, query):
        self.coll.update_many(filter, query)
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
                }
            },
            limit=count)

    def get_hosts(self, refresh_time):
        isrefresh = time.time() - refresh_time
        match = {'$match': {'$pendedtime': {'$lte': isrefresh}}}
        group = {'$group': {'_id': '$host'}}
        return self.coll.aggregate([match, group])


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
        # 提取hub网页中的链接, 新闻网页中也有“相关新闻”的链接，按需提取
        if status != 200:
            return False
        if mode == 'hub':
            newlinks = self._extract_links_re(redirected_url, html)
            goodlinks = self._filter_good(newlinks, host)
            print(f"{len(goodlinks)}/{len(newlinks)}, goodlinks/newlinks")
            return set(goodlinks)


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
            # NOTE: 9928   97618443.0   9832.6     96.1
            if not self.mongo.has({'url': url}):
                documents.append(sample)
        self.mongo.insert_many(documents)

        print(f'add links {len(documents)}')
        return True

    def crawl(self):
        # 初始化链接
        last_loading_time = time.time()
        self.hubs = self.loader.load_hubs()
        self._save_to_db(self.hubs, 'hub')
        # 开启任务
        # NOTE:500     926138.0   1852.3      0.1
        # REVIEW:
        # for _i in range(20):
        while 1:
            last_loading_time, conf, hubs = self.loader.re_load_conf(
                last_loading_time, 300)  # 刷新配置文件
            self._ischange(conf, hubs)  # 判断是否需要刷新
            tasks = self.mongo.get(self.MAX_WORKERS_CONCURRENT,
                                   self.conf['pending_threshold'])  # 获取链接
            # REVIEW:
            links_list = []
            with requests.session() as session:
                for task in tasks:  # 遍历待下载网址
                    # 抓取网页
                    # NOTE:960  504462631.0 525481.9     64.1
                    status, html, redirected_url = self.downloader.fetch(
                        session, task['url'])
                    # 更新状态
                    # NOTE: 960  108409110.0 112926.2     13.8
                    self.mongo._set_status(status, self.parser._zip_html(html),
                                           redirected_url, task['mode'],
                                           task['failure'],
                                           self.conf['failure_threshold'])
                    # 解析数据
                    # 960   27407994.0  28550.0      3.5
                    links = self.parser.process(status, html, redirected_url,
                                                task['mode'], task['host'])
                    if links:
                        links_list.extend(links)

                    # 添加数据
                    # NOTE: 20  112281095.0 5614054.8     17.6
                # REVIEW:

                # lp_wrap = lp(self._save_to_db)
                # lp_wrap(links_list, 'url')

                self._save_to_db(links_list, 'url')

    def run(self):
        try:
            self.crawl()
            # REVIEW:
            # lp.print_stats()
        except KeyboardInterrupt:
            print('stopped by yourself!')
        self.close()


if __name__ == '__main__':
    from line_profiler import LineProfiler
    lp = LineProfiler()
    news_crawler = Crawler()
    news_crawler.run()