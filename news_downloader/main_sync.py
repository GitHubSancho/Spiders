#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_sync.py
#CREATE_TIME: 2022-07-26
#AUTHOR: Sancho
"""
调度器
采集效率:41条/分钟
"""

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
import config


class NewsCrawlerSync:
    def __init__(self) -> None:
        # 读取配置
        self.dir = sys.path[0]
        self.name = self.dir.split('\\')[-1]
        self.path = self.dir + '\\' + self.name
        self.cfg = config.Config(self.path + '.cfg')
        self.hubs = self.cfg['hubs.hubs']
        self.hub_hosts = [urlparse.urlparse(i).netloc
                          for i in self.hubs]  # 取出hubs的域名
        self.logger = downloader.init_file_logger(self.path + '.log')

        # 连接数据库
        self.db = ezpymongo.Connection(self.cfg)
        self.urlpool = UrlPool(self.db, self.cfg, self.path)

    def close(self):
        self.cfg.close()

    def __del__(self):
        self.close()

    def save_to_db(self, url, html):
        # 文件编码
        if isinstance(url, str):  # 判断数据是否是字符串
            url = url.encode('utf8')
        if isinstance(html, str):
            html = html.encode('utf8')
        html_lzma = lzma.compress(html)  # 压缩文件
        good = False

        # FIXME:优化写入和检索效率，采用hash算法
        try:
            if self.db.get_one({"url": url}):
                self.db.update({"url": url}, {"$set": {"html": html_lzma}})
            else:
                self.db.insert({"url": url, "html": html_lzma})
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
        while 1:
            if time.time() - stime > 300:
                # 每运行五分钟读取配置
                self.cfg.close()
                self.cfg = config.Config(self.path + '.cfg')
                self.hubs = self.cfg['hubs.hubs']
                self.hub_hosts = [
                    urlparse.urlparse(i).netloc for i in self.hubs
                ]  # 取出hubs的域名
                stime = time.time()  # 更新时间
                self.exit = self.cfg['exit.exit']
                if self.exit == 1:
                    self.close()  # 退出程序
                    break

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