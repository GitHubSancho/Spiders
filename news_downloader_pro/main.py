#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_thread.py
#CREATE_TIME: 2022-08-09
#AUTHOR: Sancho
"""
新闻爬虫
pandas数据结构版
效率：70条/分钟
NOTE:在抓取网页和添加链接到网址池时耗费绝大部分时间
- 访问网页需要等待
- 创建pandas对象和更新浪费时间
"""

from statistics import mean
import pandas
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
#TODO:
from concurrent.futures import ThreadPoolExecutor
# REVIEW:
import traceback


class Loader:
    def __init__(self) -> None:
        my_dir = sys.path[0]
        my_name = my_dir.split('\\')[-1]
        self.path = f'{my_dir}\\{my_name}'

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

    def _dump_cache(self, urls):
        with contextlib.suppress(Exception):
            with open(f'{self.path}.pkl', 'wb') as f:
                pickle.dump(urls, f)
                return True

    def _load_cache(self):
        with contextlib.suppress(Exception):
            with open(f'{self.path}.pkl', 'rb') as f:
                return pickle.load(f)


class Mongo:
    STATUS_FAILURE = b'0'
    STATUS_SUCCESS = b'1'

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

    def save_to_db(self, url, html, issave, status):
        if status != 200 or issave == False:
            return False
        # 压缩数据
        if isinstance(html, str):
            html = html.encode('utf8')
        html_lzma = lzma.compress(html)
        good = False
        try:
            if self.coll.find_one({'url': url}):
                print(f"failure save to db: {url}, status: {status}")
                return good
            self.coll.insert_one({
                'url': url,
                'status': status,
                'html': html_lzma
            })
        except Exception as e:
            if e.args[0] == 1062:
                good = True
            else:
                traceback.print_exc()
                raise e
        return good


class UrlManager:
    def __init__(self, coll, conf) -> None:
        self.coll = coll
        self.conf = conf
        self.columns_list = ['host', 'status', 'pendedtime', 'failure', 'mode']
        self.url_pool = pandas.DataFrame([], columns=self.columns_list)

    def _add(self, url_pool, url, mode):
        if not isinstance(url, str):
            return url_pool, False
        # 检查子域名
        host = urlparse.urlparse(url).netloc
        if not host or '.' not in host:
            print('bad url:', url)
            return url_pool, False
        # 是否存在网址池
        if url in url_pool.index:
            # 存在数据库中则不入库
            if self.coll.find_one({'url': url
                                   }) and url_pool.loc[url, 'mode'] == 'url':
                return url_pool, False
            # 已存在网址池且超过刷新时间，更新状态
            refresh_mode = "pending_threshold" if mode == 'url' else "hub_refresh_span"
            if time.time(
            ) - url_pool.loc[url]["pendedtime"] > self.conf[refresh_mode]:
                url_pool.loc[url]["status"] = "waiting"
                url_pool.loc[url]["pendedtime"] = 0
                return url_pool, True
            # 已存在网址池且没有超过刷新时间，不更新
            return url_pool, False
        # 不存在网址池，添加
        df = pandas.DataFrame([[host, "waiting", 0, 0, mode]],
                              columns=self.columns_list,
                              index=[url])
        url_pool = url_pool.append(df)
        return url_pool, True

    def add_many(self, url_pool, urls: list, mode='url'):
        if not urls:
            return url_pool
        elif isinstance(urls, str):
            self._add(url_pool, urls, mode)
            return url_pool
        for url in urls:
            url_pool, _ = self._add(url_pool, url, mode)
        return url_pool

    def _query(self, r, refresh_mode, box, box_count):
        isrefresh = time.time() - r['pendedtime'] > self.conf[refresh_mode]
        isout = len(box) < box_count
        if isrefresh and isout:
            box.append(r.name)
            r['pendedtime'] = time.time()
            r['status'] = 'pending'
            return r, box, True
        return r, box, False

    def pop(self, url_pool, count, hub_percent=34, limit=True):
        urls, hubs = [], []
        # 计算需求量
        hubs_count = count * hub_percent // 100
        urls_count = count - hubs_count
        # 遍历网址池
        for _, group in url_pool.groupby('host'):
            for url, r in group.iterrows():
                if r["mode"] == 'hub':
                    r, hubs, sign = self._query(r, 'hub_refresh_span', hubs,
                                                hubs_count)
                elif r["mode"] == 'url':
                    urls_count = count - hubs_count  # 动态调整需求量
                    r, urls, sign = self._query(r, 'pending_threshold', urls,
                                                urls_count)
                if limit and sign:
                    break
            if len(hubs) + len(urls) >= count:
                break
        return hubs + urls, url_pool

    def set_status(self, url_pool, redirected_url, url, status_code):
        """访问链接后设置状态"""
        # 如果返回链接和链接不相等，采用返回链接
        if redirected_url != url:
            url = redirected_url
        # 简单判断主机地址是否合法
        host = urlparse.urlparse(url).netloc  # 解析到主机地址
        if not host or '.' not in host:
            print('地址错误:', url, ', len of ur:', len(url))
            return url_pool, False
        # 更新链接状态
        if url not in url_pool.index:  # 如果不存在网址池则不更新此链接
            return url_pool, False
        if status_code == 200:  # 如果成功访问，则从网址池移除，并标记在数据库中记录
            if url_pool.loc[url]["mode"] == 'url':
                url_pool = url_pool.drop([url])
                return url_pool, True
            # 如果成功访问，更新hub链接的状态
            url_pool.loc[url]['status'] = 'waiting'
            url_pool.loc[url]['pendedtime'] = time.time()
            return url_pool, True
        elif status_code == 404:  # 如果地址不存在，则从网址池移除，并标记在数据库中记录
            url_pool = url_pool.drop([url])
            return url_pool, True
        elif url_pool.loc[url]['failure'] > 0:  # 访问失败时
            url_pool.loc[url]['failure'] += 1  # 记录失败次数+1
            if url_pool.loc[url]["mode"] == 'url' and url_pool.loc[url][
                    'failure'] > self.conf['failure_threshold']:  # 如果失败次数超过上限
                url_pool = url_pool.drop([url])  # 从网址池移除
                return url_pool, True  # 标记在数据库中记录
            else:  # 如果失败次数没有超过上限,则修改状态
                url_pool.loc[url]['status'] = 'waiting'
        else:  # 第一次失败，则修改状态
            url_pool.loc[url]['failure'] = 1  # 记录失败次数
            url_pool.loc[url]['status'] = 'waiting'
        return url_pool, False


class Downloader:
    def __init__(self) -> None:
        pass

    def fetch(self, session, url, headers=None, timeout=9):
        # TODO:UA池
        _headers = headers or {
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
        }

        try:
            resp = session.get(url, headers=_headers, timeout=timeout)
            status = resp.status_code
            self.encoding = "utf-8"
            html = resp.text
            redirected_url = str(resp.url)
            print(f'succes download:{url} | status:{status}')
        except Exception as e:
            msg = f'Failed download: {url} | exception: {str(type(e))}, {str(e)}'
            print(msg)
            html = ''
            status = 0
            redirected_url = url
        return status, html, redirected_url


class Parser:
    def __init__(self) -> None:
        self.G_BIN_POSTFIX = ('exe', 'doc', 'docx', 'xls', 'xlsx', 'ppt',
                              'pptx', 'pdf', 'jpg', 'png', 'bmp', 'jpeg',
                              'gif', 'zip', 'rar', 'tar', 'bz2', '7z', 'gz',
                              'flv', 'mp4', 'avi', 'wmv', 'mkv', 'apk')
        self.G_NEWS_POSTFIX = ('.html?', '.htm?', '.shtml?', '.shtm?')
        self.G_PATTERN_TAG_A = re.compile(
            r'<a[^>]*?href=[\'"]?([^> \'"]+)[^>]*?>(.*?)</a>',
            re.I | re.S | re.M)

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

    def process(self, status, html, redirected_url, ishub, hosts):
        # 提取hub网页中的链接, 新闻网页中也有“相关新闻”的链接，按需提取
        if status != 200:
            return False
        if ishub:
            newlinks = self._extract_links_re(redirected_url, html)
            goodlinks = self._filter_good(newlinks, hosts)
            print(f"{len(goodlinks)}/{len(newlinks)}, goodlinks/newlinks")
            return set(goodlinks)


class Crawler:
    def __init__(self) -> None:
        # 读取配置文件
        self.loader = Loader()
        self.conf = self.loader.load_conf()
        self.hubs = self.loader.load_hubs()
        # 连接数据库
        self.mongo = Mongo(self.conf)
        self.db = self.mongo.db
        self.coll = self.mongo.coll
        # 载入网址池
        self.url_manager = UrlManager(self.coll, self.conf)
        self.url_pool = self.url_manager.url_pool
        self.url_pool = self.url_manager.add_many(self.url_pool, self.hubs,
                                                  "hub")
        cache = self.loader._load_cache()
        self.url_pool = self.url_manager.add_many(self.url_pool, cache)
        # 其它功能
        self.downloader = Downloader()
        self.parser = Parser()

    def close(self):
        # 写入配置文件
        # self.loader._dump_hubs(self.hubs)
        # self.loader._dump_cache(self.urls)
        # 关闭数据库
        self.mongo.client.close()
        # 程序退出
        sys.exit()

    def __del__(self):
        self.close()

    def _re_load_conf(self, last_loading_time, refresh_time=300):
        if time.time() - last_loading_time > refresh_time:
            # 读取配置信息
            self.conf = self.loader.load_conf()
            # 退出检测
            if self.conf['exit'] == True:
                self.close()
            return time.time()
        return last_loading_time

    def crawl(self):
        last_loading_time = time.time()
        self._concurrent_workers = 0
        while True:
            # 刷新配置文件
            last_loading_time = self._re_load_conf(
                last_loading_time)  # 重新获取配置信息
            # 取出待下载链接
            tasks, self.url_pool = self.url_manager.pop(
                self.url_pool, MAX_WORKERS_CONCURRENT)
            if not tasks:
                print('no url to crawl, sleep')
                time.sleep(1)
                continue
            # 遍历下载链接
            for url in tasks:
                with requests.session() as session:
                    status, html, redirected_url = self.downloader.fetch(
                        session, url)  # 抓取网页
                    self.url_pool, ifsave = self.url_manager.set_status(
                        self.url_pool, redirected_url, url, status)  # 设置状态
                    if ishub := url in self.url_pool[self.url_pool['mode'] ==
                                                     'hub'].index:
                        links = self.parser.process(
                            status, html, redirected_url, ishub,
                            set(self.url_pool['host'].tolist()))  # 解析数据
                        self.url_pool = self.url_manager.add_many(
                            self.url_pool, links)
                    else:
                        self.mongo.save_to_db(url, html, ifsave, status)

    def run(self):
        try:
            self.crawl()
        except KeyboardInterrupt:
            print('stopped by yourself!')
            self.close()


if __name__ == '__main__':
    MAX_WORKERS_PROCESS, MAX_WORKERS_THREAD, MAX_WORKERS_CONCURRENT = 6, 12, 24
    news_crawler = Crawler()
    news_crawler.run()
