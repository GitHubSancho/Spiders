#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main.py
#CREATE_TIME: 2022-08-07
#AUTHOR: Sancho
"""
调度器(多线程、多进程、协程)
抓取效率:946/分钟
"""

#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main_async.py
#CREATE_TIME: 2022-08-03
#AUTHOR: Sancho
"""
异步调度器
采集效率:91条/分钟
"""

import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import time
import urllib.parse as urlparse
from motor.motor_asyncio import AsyncIOMotorClient  # 异步操作mongo模块
import asyncio  # 异步操作文件读写
import aiohttp  # 异步操作网络请求
import traceback
import lzma
import farmhash
import yaml
import multiprocessing
import url_manager
import db_manager
import url_downloader
# import uvloop # 不适用windows
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class NewsCrawler:
    def __init__(self, concurrent_workers_max=30) -> None:
        self._concurrent_workers = 0
        self._concurrent_workers_max = concurrent_workers_max

        # 读取配置
        dir = sys.path[0]
        name = dir.split('\\')[-1]
        self.path = dir + '\\' + name
        with open(self.path + '.yml', 'r', encoding='utf-8') as f:
            self.cfg = yaml.load(f, Loader=yaml.CLoader)
        # with open(self.path + '_hubs.yml', 'r', encoding='utf-8') as f:
        #     self.hubs = yaml.load(f, Loader=yaml.CLoader)['hubs']
        #     print('loading hubs:{}'.format(self.hubs))
        # 连接数据库
        self.logger = self.init_file_logger(self.path + '.log')
        self.db = db_manager.DB(self.cfg, self.logger)
        self.col = self.db.col
        self.url_pool = url_manager.Url_Pool(self.path, self.db, self.cfg)

    async def load_yml(self):
        async with open(self.path + '.yml', 'r', encoding='utf-8') as f:
            self.cfg = await yaml.load(f, Loader=yaml.CLoader)
        return True

    def init_file_logger(self, fname):
        ch = TimedRotatingFileHandler(
            fname, when="midnight")  # 将日志消息发送到磁盘文件，并支持日志文件按时间切割
        ch.setLevel(logging.INFO)  # 默认警报等级
        fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'  # 格式化
        formatter = logging.Formatter(fmt)
        ch.setFormatter(formatter)  # 添加格式化到配置
        logger = logging.getLogger(fname)  # 获取日志对象
        logger.addHandler(ch)  # 添加配置到记录器
        return logger

    def filter_good(self, urls):
        goodlinks = []
        for url in urls:
            host = urlparse.urlparse(url).netloc
            hub_hosts = [
                urlparse.urlparse(hub).netloc for hub in self.url_pool.hubs
            ]
            if host in hub_hosts:
                goodlinks.append(url)
        return goodlinks

    async def process(self, url, ishub):
        async with aiohttp.ClientSession(loop=self.loop) as self.session:
            status, html, redirected_url = await url_downloader.fetch(
                self.session, url)
        self.url_pool.set_status(url, status)
        if redirected_url != url:
            self.url_pool.set_status(redirected_url, status)
        # 提取hub网页中的链接, 新闻网页中也有“相关新闻”的链接，按需提取
        if status != 200:
            self._concurrent_workers -= 1
            return
        if ishub:
            newlinks = url_downloader.extract_links_re(redirected_url, html)
            goodlinks = self.filter_good(newlinks)
            print("%s/%s, goodlinks/newlinks" %
                  (len(goodlinks), len(newlinks)))
            self.url_pool.addmany(goodlinks)
        else:
            await self.db.save_to_db(redirected_url, html)
        self._concurrent_workers -= 1

    def close(self):
        self.url_pool.close()
        self.loop.close()

    def __del__(self):
        self.close()

    async def loop_crawl(self):
        last_rating_time = time.time()
        last_loading_time = time.time()
        counter = 0
        while 1:
            # 输出当前协程状态
            if self._concurrent_workers > self._concurrent_workers_max:
                print(
                    '====== got workers_max, sleep 1 sec to next worker =====')
                await asyncio.sleep(1)
                continue
            gap = time.time() - last_rating_time
            if gap > 5:
                rate = counter / gap
                print('\tloop_crawl() rate:%s, counter: %s, workers: %s' %
                      (round(rate, 2), counter, self._concurrent_workers))
                last_rating_time = time.time()
                counter = 0

            # 每运行五分钟读取配置
            if time.time() - last_loading_time > 300:
                await self.load_yml()
                # self.hub_hosts = [
                #     urlparse.urlparse(i).netloc for i in self.hubs
                # ]  # 取出hubs的域名
                last_loading_time = time.time()  # 更新时间
                if self.cfg['exit'] == True:  # 退出程序
                    self.close()
                    break

            # 创建任务进入下载
            tasks = self.url_pool.pop(self._concurrent_workers_max,
                                      self.cfg['hub_refresh_span'],
                                      self.cfg['pending_threshold'])
            if not tasks:
                print('no url to crawl, sleep')
                await asyncio.sleep(1)
                continue
            for url, ishub in tasks:
                self._concurrent_workers += 1
                counter += 1
                # print('crawl:', url)
                asyncio.ensure_future(self.process(url, ishub))

    def run(self):
        try:
            # 创建异步任务
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(self.loop_crawl())
        except KeyboardInterrupt:
            print('stopped by yourself!')
            del self
            pass


if __name__ == '__main__':
    CONCURRENT_WORKER_MAX = 30
    nc = NewsCrawler()
    nc.run()
    print("done!")
