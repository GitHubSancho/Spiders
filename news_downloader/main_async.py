#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_async.py
#CREATE_TIME: 2022-08-03
#AUTHOR: Sancho
"""
异步调度器
采集效率:91条/分钟
"""

import sys
import time
import urllib.parse as urlparse
from motor.motor_asyncio import AsyncIOMotorClient  # 异步操作mongo模块
import asyncio  # 异步操作文件读写
import aiohttp  # 异步操作网络请求
import traceback
import lzma
from urlpool import UrlPool
import downloader
import config  # 读取配置模块
# import sanicdb # 此模块是Python异步操作Mysql框架
# import farmhash
# import uvloop # 不适用windows
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class NewsCrawlerAsync:
    def __init__(self):
        self._workers = 0
        self._workers_max = 30

        # 读取配置
        self.dir = sys.path[0]
        self.name = self.dir.split('\\')[-1]
        self.path = self.dir + '\\' + self.name
        self.cfg = config.Config(self.path + '.cfg')
        self.logger = downloader.init_file_logger(self.path + '.log')

        # 连接数据库
        if self.cfg['user'] and self.cfg['password']:
            self.client = AsyncIOMotorClient(
                'mongodb://%s:%d@%s:%d' %
                (self.cfg['user'], self.cfg['password'], self.cfg['host'],
                 self.cfg['port']))
        else:
            self.client = AsyncIOMotorClient(self.cfg['host'],
                                             self.cfg['port'])
        self.db = self.client[self.cfg['database']]
        self.col = self.db[self.cfg['collection']]
        self.urlpool = UrlPool(self.db, self.cfg, self.path)

    def load_hubs(self, ):
        self.hub_hosts = set()
        self.hubs = self.cfg['hubs.hubs']
        for hub in self.hubs:
            host = urlparse.urlparse(hub).netloc
            self.hub_hosts.add(host)
        self.urlpool.set_hubs(self.hubs)

        return

    async def save_to_db(self, url, html):
        # FIXME:使用hash算法
        # 压缩数据
        if isinstance(url, str):  # 判断数据是否是字符串
            url = url.encode('utf8')
        if isinstance(html, str):
            html = html.encode('utf8')
        sql = {'url': url}
        html_lzma = lzma.compress(html)
        sql2 = {'html': html_lzma}
        # 存储数据
        good = False
        try:
            good = True
            d = await self.col.find_one(sql)
            if d:  # 是否存在数据库
                sql2 = {'$set': sql2}
                await self.col.update_one(sql, sql2)
                return good
            sql.update(sql2)
            await self.col.insert_one(sql)
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

    async def process(self, url, ishub):
        # self.session = aiohttp.ClientSession(loop=self.loop)
        async with aiohttp.ClientSession(loop=self.loop) as self.session:
            status, html, redirected_url = await downloader.downloader_async(
                self.session, url)
        self.urlpool.set_status(url, status)
        if redirected_url != url:
            self.urlpool.set_status(redirected_url, status)
        # 提取hub网页中的链接, 新闻网页中也有“相关新闻”的链接，按需提取
        if status != 200:
            self._workers -= 1
            return
        if ishub:
            newlinks = downloader.extract_links_re(redirected_url, html)
            goodlinks = self.filter_good(newlinks)
            print("%s/%s, goodlinks/newlinks" %
                  (len(goodlinks), len(newlinks)))
            self.urlpool.addmany(goodlinks)
        else:
            await self.save_to_db(redirected_url, html)
        self._workers -= 1

    def close(self):
        del self.urlpool
        self.cfg.close()
        self.loop.close()

    def __del__(self):
        self.close()

    async def loop_crawl(self, ):
        self.load_hubs()
        last_rating_time = time.time()
        stime = time.time()
        counter = 0
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
                    del self  # 退出程序
                    break
            tasks = self.urlpool.pop(self._workers_max)
            if not tasks:
                print('no url to crawl, sleep')
                await asyncio.sleep(1)
                continue
            for url, ishub in tasks:
                self._workers += 1
                counter += 1
                print('crawl:', url)
                asyncio.ensure_future(self.process(url, ishub))

            gap = time.time() - last_rating_time
            if gap > 5:
                rate = counter / gap
                print('\tloop_crawl() rate:%s, counter: %s, workers: %s' %
                      (round(rate, 2), counter, self._workers))
                last_rating_time = time.time()
                counter = 0
            if self._workers > self._workers_max:
                print(
                    '====== got workers_max, sleep 1 sec to next worker =====')
                await asyncio.sleep(1)

    def run(self):
        try:
            # 创建异步任务
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(self.loop_crawl())
        except KeyboardInterrupt:
            print('stopped by yourself!')
            del self.urlpool
            pass


if __name__ == '__main__':
    nc = NewsCrawlerAsync()
    nc.run()
