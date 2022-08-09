#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main.py
#CREATE_TIME: 2022-08-07
#AUTHOR: Sancho
"""
调度器(多线程、多进程、协程)
抓取效率:946/分钟
"""

import logging
from logging.handlers import TimedRotatingFileHandler
from pickle import NONE
import sys
import time
from tkinter.tix import Tree
import requests
import urllib.parse as urlparse
from motor.motor_asyncio import AsyncIOMotorClient  # 异步操作mongo模块
# import asyncio  # 异步操作文件读写
# import aiohttp  # 异步操作网络请求
import traceback
import lzma
import farmhash
import yaml
# import multiprocessing
import url_manager
import db_manager
import url_downloader
import html_parser
from concurrent.futures import ThreadPoolExecutor, as_completed

# import uvloop # 不适用windows
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class NewsCrawler:
    def __init__(self, concurrent_workers_max=30) -> None:
        my_dir = sys.path[0]
        my_name = my_dir.split('\\')[-1]
        self.path = f'{my_dir}\\{my_name}'
        self._concurrent_workers = 0

        self.cfg = self._load_cfg()  # 读取配置文件
        self.logger = self._init_file_logger(f'{self.path}.log')
        self.db = self._login_db()  # 连接服务器
        self.col = self.db.col
        self.url_pool = url_manager.Url_Pool(self.path, self.db,
                                             self.cfg)  # 读取链接池

    def close(self):
        self.url_pool.close()
        sys.exit()

    def _load_cfg(self):
        with open(f'{self.path}.yml', 'r', encoding='utf-8') as f:
            cfg = yaml.load(f, Loader=yaml.CLoader)
        return cfg

    def _init_file_logger(self, fname):
        ch = TimedRotatingFileHandler(
            fname, when="midnight")  # 将日志消息发送到磁盘文件，并支持日志文件按时间切割
        ch.setLevel(logging.INFO)  # 默认警报等级
        fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'  # 格式化
        formatter = logging.Formatter(fmt)
        ch.setFormatter(formatter)  # 添加格式化到配置
        logger = logging.getLogger(fname)  # 获取日志对象
        logger.addHandler(ch)  # 添加配置到记录器
        return logger

    def _login_db(self):
        return db_manager.DB(self.cfg, self.logger)

    def _print_info(self):
        if self._concurrent_workers > MAX_WORKERS_CONCURRENT:
            print('====== got workers_max, sleep 1 sec to next worker =====')
            time.sleep(1)
        gap = time.time() - self.last_rating_time
        if gap > 5:
            rate = self.counter / gap
            print('\tloop_crawl() rate:%s, counter: %s, workers: %s' %
                  (round(rate, 2), self.counter, self._concurrent_workers))
            self.last_rating_time = time.time()
            self.counter = 0

    def _if_refurbish(self):
        if time.time() - self.last_loading_time > 300:
            self.cfg = self._load_cfg()
            self.last_loading_time = time.time()  # 更新时间
            if self.cfg['exit'] == True:  # 退出程序
                self.close()

    def loop_crawl(self):
        self.last_rating_time = time.time()
        self.last_loading_time = time.time()
        self.counter = 0
        while 1:
            self._print_info()
            self._if_refurbish()
            # with ThreadPoolExecutor() as thread_pool:
            #     pass
            # 读取待下载链接
            tasks = self.url_pool.pop(MAX_WORKERS_CONCURRENT,
                                      self.cfg['hub_refresh_span'],
                                      self.cfg['pending_threshold'])
            if not tasks:
                print('no url to crawl, sleep')
                time.sleep(1)
                continue

            with requests.session() as session:
                with ThreadPoolExecutor(MAX_WORKERS_THREADING) as thread_pool:

                    self._concurrent_workers += 1
                    self.counter += 1
                    [
                        thread_pool.submit(html_parser.process, session, url,
                                           ishub, self.url_pool, self.db)
                        for url, ishub in tasks
                    ]
                    self._concurrent_workers -= 1

            # for url, ishub in tasks:
            #     self._concurrent_workers += 1
            #     self.counter += 1
            #     session = requests.session()
            #     html_parser.process(session, url, ishub, self.url_pool,
            #                         self.db)
            #     self._concurrent_workers -= 1  # FIXME:此计数器未生效
            #     session.close()

    def run(self):  # sourcery skip: remove-redundant-pass
        try:
            # 创建异步任务
            # self.loop = asyncio.get_event_loop()
            # self.loop.run_until_complete(self.loop_crawl())
            self.loop_crawl()
        except KeyboardInterrupt:
            print('stopped by yourself!')
            self.close()
            pass


if __name__ == '__main__':
    MAX_WORKERS_THREADING = 12
    MAX_WORKERS_PROCESSING = 6
    MAX_WORKERS_CONCURRENT = 24
    nc = NewsCrawler()
    nc.run()
    print("done!")
