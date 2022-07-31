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


class Connection(object):
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
        """
        :Parameters:
            - `*args`:接收数据串格式,eg:"mongodb://{}:{}/"
            - `host`:数据库地址(默认'127.0.0.1'),
            - `database`:数据库名(默认'test'),
            - `collection`:数据库中的集合,
            - `user`:用户名(可选),
            - `password`:密码(可选),
            - `port`:数据库端口(默认27017)
            - `max_idle_time`:最大连接时间(7*3600分钟)
            - `timeout`:最大响应时间(默认10秒)
            - `time_zone`:设置时区(默认上海+8:00)
        :TODO: 支持读取文件连接mongo
        """
        self.host = host
        self.port = port
        self.database = database
        self.collection = collection
        self.max_idle_time = float(max_idle_time)
        self.timeoutMS = timeout * 1000
        self._last_use_time = time.time()
        self.db = None
        self.cursor = None
        self.time_zone = pytz.timezone('Asia/Shanghai')

        if user:
            self.user = parse.quote_plus(user)
        if password:
            self.password = parse.quote_plus(password)
        if time_zone:
            self.time_zone = time_zone

        if type(args) == str:
            _len = len(args.split('/'))
            if (
                    _len == 4 or _len == 3
            ) and '@' not in args:  # "mongodb://host:port/" or "mongodb://host:port"
                _ = args.split('/')[2].split(':')
                self.host = _[0]
                self.port = _[1]
                self._uri = "mongodb://{}:{}/".format(self.host, self.port)
            elif (
                    _len == 4 or _len == 3
            ) and '@' in args:  # 'mongodb://user:password@host:port/database'
                _ = args.split('/')
                _0 = _[2].split('@')
                _1 = _0[0].split(':')
                _2 = _0[1].split(':')
                self.user = parse.quote_plus(_1[0])
                self.password = parse.quote_plus(_1[1])
                self.host = _2[0]
                self.port = _2[1]
                if _len == 4 and _[3] != '':
                    self.database = _[3]
                    self._uri = "mongodb://{}:{}@{}:{}/{}".format(
                        self.user, self.password, self.host,
                        self.port)  # 不连接数据库(self.database)
                elif _len == 3 or _[
                        3] == '':  # 'mongodb://user:password@host:port/'
                    self._uri = "mongodb://{}:{}@{}:{}/{}".format(
                        self.user, self.password, self.host, self.port)
        else:
            self._uri = "mongodb://{}:{}/".format(self.host, self.port)

        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to mongoDB on %s",
                          self.host,
                          exc_info=True)

    def set_database(self, database=None):
        """设置或更换数据库"""
        if type(database) == str:
            self.database = database
        self.db_database = self.db[self.database]
        return

    def set_collection(self, collection=None):
        """设置或更换集合"""
        if type(collection) == str:
            self.collection = collection
        self.db_database_collection = self.db_database[self.collection]
        return

    def reconnect(self):
        """关闭打开的服务器并重新启动"""
        self.close()
        self.db = pymongo.MongoClient(self._uri,
                                      tz_aware=True,
                                      serverSelectionTimeoutMS=self.timeoutMS,
                                      socketTimeoutMS=self.timeoutMS,
                                      tzinfo=self.time_zone)
        self.set_database()
        self.set_collection()

        self.session = self.db.start_session()
        self.sessionID = self.session.session_id

    def close(self):
        """关闭数据库连接"""
        if getattr(self, "_db", None) is not None:
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
        if (self.db is None
                or (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
            self._last_use_time = time.time()
        return

    def _cursor(self, query, limit, batch_size):
        """处理游标（超时问题）"""
        def _refer_cursor(self):
            self._ensure_connected()  # 保证服务器是打开状态
            if time.time() - self.refreshTimestamp > 300:  # 游标打开超过五分钟
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