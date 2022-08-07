#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: db_manager.py
#CREATE_TIME: 2022-08-07
#AUTHOR: Sancho

from motor.motor_asyncio import AsyncIOMotorClient  # 异步操作mongo模块
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
            self.client = AsyncIOMotorClient(
                'mongodb://%s:%d@%s:%d' %
                (self.cfg['user'], self.cfg['password'], self.cfg['host'],
                 self.cfg['port']))
        else:
            self.client = AsyncIOMotorClient(self.cfg['host'],
                                             self.cfg['port'])
        self.db = self.client[self.cfg['database']]
        self.col = self.db[self.cfg['collection']]

    def has(self, url, _id=False):
        """判断数据是否已存在数据库"""
        # if isinstance(url, str):
        #     url = url.encode('utf8')
        try:
            if _id:
                return self.col.get_one({"_id": _id})  # 查询指定条件的数据
            return self.col.get_one({"url": url})
        except:
            pass
        return False

    def set_success(self, url):
        """添加成功的数据"""
        # if isinstance(url, str):  # 判断数据是否是字符串
        #     url = url.encode('utf8')
        try:
            if self.has(url):  # 是否存在数据库
                self.db.update({"url": url},
                               {"$set": {
                                   "status": self.STATUS_SUCCESS
                               }})
            else:
                self.db.insert({"url": url, "status": self.STATUS_SUCCESS})
            s = True
        except:
            s = False
        return s

    def set_failure(self, url):
        """添加失败的数据"""
        # if isinstance(url, str):
        #     url = url.encode('utf8')
        try:
            if self.has(url):  # 是否存在数据库
                self.db.update({"url": url},
                               {"$set": {
                                   "status": self.STATUS_FAILURE
                               }})
            else:
                self.db.insert({"url": url, "status": self.STATUS_FAILURE})
            print("failure url to db:%s" % url)
            s = True
        except:
            s = False
        return s

    async def save_to_db(self, url, html):
        # 压缩数据
        # print('save:', url)
        # if isinstance(url, str):  # 判断数据是否是字符串
        #     url = url.encode('utf8') # 编码
        if isinstance(html, str):
            html = html.encode('utf8')
        html_lzma = lzma.compress(html)
        sql = {'url': url, 'html': html_lzma}
        sql_set = {'$set': {sql['html']}}
        # 存储数据
        good = False
        try:
            d = self.has(url)
            if d:  # 是否存在数据库
                if d['url'] != url:
                    msg = 'farmhash collision: %s <=> %s' % (url, d['url'])
                    self.logger.error(msg)
                    return False
                await self.col.update_one(sql['url'], sql_set)
                good = True
                return good
            else:
                await self.col.insert_one(sql)
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