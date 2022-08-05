#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: ezpymongo.py
#CREATE_TIME: 2022-07-27
#AUTHOR: Sancho
"""
pymongo的轻量级封装
"""

import time
import logging
from bson import ObjectId
import pymongo
import pytz
from urllib import parse
import sys
import config


class Connection(object):
    def __init__(self, config, time_zone=None) -> None:
        self.time_zone = time_zone
        self.cfg = config
        # 获取服务器信息
        if self.cfg['user'] and self.cfg['password']:
            self.uri = 'mongodb://%s:%d@%s:%d' % (
                self.cfg['user'], self.cfg['password'], self.cfg['host'],
                self.cfg['port'])
        else:
            self.uri = 'mongodb://%s:%d' % (self.cfg['host'], self.cfg['port'])
        # 尝试连接服务器
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to mongoDB on %s",
                          self.cfg['host'],
                          exc_info=True)

    def set_database(self):
        """设置或更换数据库"""
        self.db_database = self.db[self.cfg["database"]]
        return self.db_database

    def set_collection(self):
        """设置或更换集合"""
        self.db_database_collection = self.db_database[self.cfg["collection"]]
        return self.db_database_collection

    def reconnect(self):
        """重新启动数据库"""
        self.close()  # 关闭数据库
        if self.time_zone == None:
            self.time_zone = pytz.timezone('Asia/Shanghai')
        self.db = pymongo.MongoClient(
            self.uri,
            tz_aware=True,
            serverSelectionTimeoutMS=self.cfg['timeoutMS'],
            socketTimeoutMS=self.cfg['timeoutMS'],
            tzinfo=self.time_zone)
        self.set_database()
        self.set_collection()

        self.session = self.db.start_session()
        self.sessionID = self.session.session_id

    def close(self):
        """关闭数据库连接"""
        if getattr(self, "db", None) is not None:
            self.db.close()
            self.db = None

    def find_database(self) -> list:
        """查询当前服务器所有数据库"""
        return self.db.list_database_names()

    def find_collection(self) -> list:
        """查询当前数据库所有集合"""
        return self.db_database.list_collection_names()

    def drop_collection(self):
        """删除当前集合"""
        return self.db_database_collection.drop()

    def get_one(self, query: dict = None) -> dict:
        """查询一条结果"""
        if query == None:
            query = {}
        return self.db_database_collection.find_one(query)

    def get_all(self,
                query: dict = None,
                limit: int = None,
                batch_size: int = None) -> list:
        """
        返回所有数据
        - `query:查询条件,需要字典对象(默认为{})
        - `limit:限制查询结果的最大数量
        - `bitch_size:查询结果按值的数量,每次调用查询下一批
        """
        if query == None:
            query = {}
        self._cursor(query, limit, batch_size)
        return self.cursor

    def _ensure_connected(self):
        """定时重启服务器与监测服务器是否为打开状态"""
        if (self.db is None or
            (time.time() - self._last_use_time > self.cfg['max_idle_time'])):
            self.reconnect()
            self._last_use_time = time.time()
        return

    def _cursor(self, query, limit, batch_size):
        """处理游标（超时问题）"""
        def _refer_cursor(self):
            self._ensure_connected()  # 保证服务器是打开状态
            if time.time() - self.refreshTimestamp > 300:  # 游标打开超过五分钟：执行重启
                self.db_database.command({"refreshSessions": [self.sessionID]})
                self.refreshTimestamp = time.time()

        self.refreshTimestamp = time.time()
        self.cursor = []
        cursor_gen = self.db_database_collection.find(
            query)  # no_cursor_timeout=True
        if limit:
            cursor_gen.limit(limit)
        for i in cursor_gen:
            _refer_cursor(self)
            self.cursor.append(i)
        cursor_gen.close()
        return

    def __del__(self):
        self.close()

    def insert(self, query):
        """插入数据,传入字典为插入一条,传入列表为插入多条"""
        _type = type(query)
        if _type != dict and _type != list:
            return False
        if isinstance(query, dict):
            return self.db_database_collection.insert_one(query)
        return self.db_database_collection.insert_many(query)

    def delete(self, query, many=False):
        """删除数据,many:是否删除所有匹配项"""
        _type = type(query)
        if _type != dict:
            return False
        if many:
            return self.db_database_collection.delete_many(query)
        return self.db_database_collection.delete_one(query)

    def update(self, document, query, many=False):
        """找到匹配的document文档并把query值更新到匹配项"""
        if type(document) != dict or type(query) != dict:
            return False
        if many:
            return self.db_database_collection.update_many(document, query)
        return self.db_database_collection.update_one(document, query)


if __name__ == '__main__':
    # 数据库管理
    connection = Connection()  # 连接mongo（默认数据库和集合）
    connection.set_database('demo')  # 重新设置数据库
    connection.set_collection('test001')  # 重新设置集合
    print(connection.find_database())  # 查看所有数据库
    print(connection.find_collection())  # 查看所有集合
    # connection.drop_collection()  # 删除当前集合

    # 查询数据
    print(connection.get_one())  # 查询结果（一条）
    print(connection.get_all())  # 返回所有数据
    # print(connection.get())  # 返回查询结果中的一条
    # print(connection.get())  # 继续查询下一条

    # 插入数据
    # document = {"name": "Sancho", "age": 23}
    # ret = connection.insert(document)  # 添加一条记录
    # document_list = [{"name": "Sancho", "age": 23}, {"name": "Leo", "age": 27}]
    # ret = connection.insert(document_list)  # 添加多条记录

    # 删除数据
    # query = {"name": "Leo"}
    # ret = connection.delete(query)  # 删除一条记录
    # query = {"_id": ObjectId("60d925e127bd4b7769251002")}
    # ret = connection.delete(query)  # 删除一条数据
    # query = {"age": {"$gt": "23"}}
    # ret = connection.delete(query, many=True)  # 删除多条数据

    # 更新文档
    # query = {"name": "Sancho"}
    # connection.update(query, {"$set": {"age": 18}})  # 更新一条文档
    # query = {"name": "Sancho", "age": {"$gt": 18}}
    # connection.update(query, {"$set": {"age": 18}}, True)  # 更新所有文档