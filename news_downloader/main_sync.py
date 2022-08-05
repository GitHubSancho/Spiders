#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_sync.py
#CREATE_TIME: 2022-07-26
#AUTHOR: Sancho
"""调度器"""

import urllib.parse as urlparse
import lzma
import farmhash
import traceback
import downloader
import json
import sys
import ezpymongo
import time
from urlpool import UrlPool

# class NewsCrawlerSync:
#     def __init__(self, name):
#         self.db = Connection(config.db_host, config.db_db, config.db_user,
#                             config.db_password)
#         self.logger = fn.init_file_logger(name + '.log')
#         self.urlpool = UrlPool(name)
#         self.hub_hosts = None
#         self.load_hubs()


class NewsCrawlerSync:
    def __init__(self,
                 *args: str,
                 host='127.0.0.1',
                 database='demo',
                 collection='test001',
                 user=None,
                 password=None,
                 port=27017,
                 max_idle_time=7 * 3600,
                 timeout=10,
                 time_zone=None):
        # TODO:文件读取服务器配置
        # 连接服务器
        self.db = ezpymongo.Connection(host=host,
                                       database=database,
                                       collection=collection,
                                       user=user,
                                       password=password,
                                       port=port,
                                       max_idle_time=max_idle_time,
                                       timeout=timeout,
                                       time_zone=time_zone)
        self.urlpool = UrlPool(self.db, database)
        # 读取配置
        self.name = database
        self.path = sys.path[0] + '\\' + self.name  # 当前文件路径
        self.logger = downloader.init_file_logger(self.path + '.log')

    def _load_ini(self):
        """读取配置文件"""
        path = self.path + '.json'  # 当前文件目录下
        try:
            with open(path, 'r') as f:
                self.ini = json.load(f)
                self.hub_hosts = [
                    urlparse.urlparse(i).netloc
                    for i in self.ini.get("hubs", None)
                ]
        except:
            pass

    # def load_hubs(self, ):
    #     sql = 'select url from crawler_hub'
    #     data = self.db.query(sql)
    #     self.hub_hosts = set()
    #     hubs = []
    #     for d in data:
    #         host = urlparse.urlparse(d['url']).netloc
    #         self.hub_hosts.add(host)
    #         hubs.append(d['url'])
    #     self.urlpool.set_hubs(hubs, 300)

    # def load_hubs(self, ):
    #     hubs = self.ini.get("hubs", None)
    #     if hubs == None:
    #         print("访问链接前请先设置hubs链接")
    #         return False
    #     print("load hubs:%s" % hubs)

    def save_to_db(self, url, html):
        # d = self.db.get_one({"url": url})
        # if d:
        #     if d['url'] != url:
        #         msg = 'farmhash collision: %s <=> %s' % (url, d['url'])
        #         self.logger.error(msg)
        #     return True
        if isinstance(html, str):
            html = html.encode('utf8')
        # html_lzma = lzma.compress(html)  # 压缩文件
        # sql = ('insert into crawler_html(urlhash, url, html_lzma) '
        #        'values(%s, %s, %s)')
        good = False
        try:
            if self.db.get_one({"url": url}):
                self.db.update({"url": url}, {"$set": {"html": html}})
            else:
                self.db.insert({"url": url, "html": html})
            good = True
        except Exception as e:
            if e.args[0] == 1062:
                # Duplicate entry
                good = True
                pass
            else:
                traceback.print_exc()
                raise e
        return good

    def filter_good(self, urls):
        goodlinks = []
        for url in urls:
            host = urlparse.urlparse(url).netloc
            if host in self.hub_hosts:
                goodlinks.append(url)
        return goodlinks

    def process(self, url, ishub):
        status, html, redirected_url = downloader.downloader(url)
        print("complete:%s,%sByte,%s" %
              (status, len(html.encode()), redirected_url))
        self.urlpool.set_status(url, status)
        if redirected_url != url:
            self.urlpool.set_status(redirected_url, status)
        # 提取hub网页中的链接, 新闻网页中也有“相关新闻”的链接，按需提取
        if status != 200:
            return
        if ishub:
            newlinks = downloader.extract_links_re(redirected_url,
                                                   html)  # 解析网页内地址
            goodlinks = self.filter_good(newlinks)
            print("%s/%s, goodlinks/newlinks" %
                  (len(goodlinks), len(newlinks)))
            self.urlpool.addmany(goodlinks)
        else:
            self.save_to_db(redirected_url, html)

    def run(self, links: int):
        stime = time.time()
        self._load_ini()
        while 1:
            if time.time() - stime > 300:
                # 每运行五分钟读取配置
                self._load_ini()
                stime = time.time()
                if self.ini.get("exit", 0) == 1:
                    self.close()  # 退出程序

            urls = self.urlpool.pop(links)
            if urls == []:
                # 没有链接可下载时
                time.sleep(1)  # 等待1秒
                continue
            for url, ishub in urls:
                self.process(url, ishub)


if __name__ == '__main__':
    crawler = NewsCrawlerSync()
    crawler.run(5)