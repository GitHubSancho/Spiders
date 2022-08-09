#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: db_manager.py
#CREATE_TIME: 2022-08-07
#AUTHOR: Sancho

import asyncio
import contextlib
# from motor.motor_asyncio import AsyncIOMotorClient  # 异步操作mongo模块
from pymongo.mongo_client import MongoClient
import lzma
import traceback


class DB:
    STATUS_FAILURE = b'0'
    STATUS_SUCCESS = b'1'

    def __init__(self, cfg, logger) -> None:
        # 连接数据库
        # FIXME:设置时区
        self.cfg = cfg
        self.logger = logger
        if self.cfg['user'] and self.cfg['password']:
            # self.client = AsyncIOMotorClient(
            #     'mongodb://%s:%d@%s:%d' %
            #     (self.cfg['user'], self.cfg['password'], self.cfg['host'],
            #      self.cfg['port']))
            self.client = MongoClient('mongodb://%s:%d@%s:%d' %
                                      (self.cfg['user'], self.cfg['password'],
                                       self.cfg['host'], self.cfg['port']))
        else:
            # self.client = AsyncIOMotorClient(self.cfg['host'],
            #                                  self.cfg['port'])
            self.client = MongoClient('mongodb://%s:%d' %
                                      (self.cfg['host'], self.cfg['port']))
        self.db = self.client[self.cfg['database']]
        self.col = self.db[self.cfg['collection']]

    def has(self, url, _id=False):
        """判断数据是否已存在数据库"""
        try:
            if _id:
                return self.col.find_one({"_id": _id})
            x = self.col.find_one({"url": url})
            return x
        except Exception:
            return False

    def set_success(self, url):
        """添加成功的数据"""
        try:
            if saved := self.has(url):
                self.col.update_one(saved,
                                    {"$set": {
                                        "status": self.STATUS_SUCCESS
                                    }})
            else:
                self.col.insert_one({
                    "url": url,
                    "status": self.STATUS_SUCCESS
                })
            s = True
        except Exception as e:
            s = False
        return s

    def set_failure(self, url):
        """添加失败的数据"""
        try:
            if saved := self.has(url):
                self.col.update_one(saved,
                                    {"$set": {
                                        "status": self.STATUS_FAILURE
                                    }})
            else:
                self.col.insert_one({
                    "url": url,
                    "status": self.STATUS_FAILURE
                })
            print(f"failure url to db:{url}")
            s = True
        except Exception:
            s = False
        return s

    def save_to_db(self, url, html):
        if isinstance(html, str):
            html = html.encode('utf8')
        html_lzma = lzma.compress(html)
        sql = {'url': url, 'html': html_lzma}
        sql_set = {'$set': {'html': html_lzma}}
        good = False
        try:
            if saved := self.has(url):
                self.col.update_one(saved, sql_set)
                good = True
                return good
            else:
                self.col.insert_one(sql)
            good = True
        except Exception as e:
            if e.args[0] == 1062:
                good = True
            else:
                traceback.print_exc()
                raise e
        return good