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
import traceback
import pymysql
import pymysql.cursors
import pymongo
import pytz
from bson import ObjectId
from urllib import parse


class Connection(object):
    # def __init__(self,
    #              host,
    #              database,
    #              user=None,
    #              password=None,
    #              port=0,
    #              max_idle_time=7 * 3600,
    #              connect_timeout=10,
    #              time_zone="+0:00",
    #              charset="utf8mb4",
    #              sql_mode="TRADITIONAL"):
    #     self.host = host
    #     self.database = database
    #     self.max_idle_time = float(max_idle_time)

    #     args = dict(use_unicode=True,
    #                 charset=charset,
    #                 database=database,
    #                 init_command=('SET time_zone = "%s"' % time_zone),
    #                 cursorclass=pymysql.cursors.DictCursor,
    #                 connect_timeout=connect_timeout,
    #                 sql_mode=sql_mode)
    #     if user is not None:
    #         args["user"] = user
    #     if password is not None:
    #         args["passwd"] = password

    #     # We accept a path to a MySQL socket file or a host(:port) string
    #     if "/" in host:
    #         args["unix_socket"] = host
    #     else:
    #         self.socket = None
    #         pair = host.split(":")
    #         if len(pair) == 2:
    #             args["host"] = pair[0]
    #             args["port"] = int(pair[1])
    #         else:
    #             args["host"] = host
    #             args["port"] = 3306
    #     if port:
    #         args['port'] = port

    #     self._db = None
    #     self._db_args = args
    #     self._last_use_time = time.time()
    #     try:
    #         self.reconnect()
    #     except Exception:
    #         logging.error("Cannot connect to MySQL on %s",
    #                       self.host,
    #                       exc_info=True)

    def __init__(
        self,
        *args: str,
        host='127.0.0.1',
        database='demo',
        collection='test001',
        user=None,
        password=None,
        port=27017,
        max_idle_time=7 * 3600,
        timeout=5,
    ):
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
            - `timeout`:最大响应时间(默认5秒)
        :TODO: 支持读取文件连接mongo
        :TODO: 支持时区
        """
        self.host = host
        self.port = port
        self.database = database
        self.collection = collection
        self.max_idle_time = float(max_idle_time)
        self.timeoutMS = timeout * 1000
        self._last_use_time = time.time()
        self.db = None

        if user:
            self.user = parse.quote_plus(user)
        if password:
            self.password = parse.quote_plus(password)

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

    def reconnect(self):
        """关闭打开的服务器并重新启动"""
        # self.close()
        # self._db = pymysql.connect(**self._db_args)
        # self._db.autocommit(True) # 二次确认

        self.close()
        self.db = pymongo.MongoClient(self._uri)

    def close(self):
        """关闭数据库连接"""
        if getattr(self, "_db", None) is not None:
            self.db.close()
            self.db = None

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if (self._db is None
                or (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def __del__(self):
        self.close()

    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            result = cursor.fetchall()
            return result
        finally:
            cursor.close()

    def get(self, query, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query.
        """
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            return cursor.fetchone()
        finally:
            cursor.close()

    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            cursor.execute(query, kwparameters or parameters)
            return cursor.lastrowid
        except Exception as e:
            if e.args[0] == 1062:
                pass
            else:
                traceback.print_exc()
                raise e
        finally:
            cursor.close()

    insert = execute

    ## =============== high level method for table ===================

    def table_has(self, table_name, field, value):
        if isinstance(value, str):
            value = value.encode('utf8')
        sql = 'SELECT %s FROM %s WHERE %s="%s"' % (field, table_name, field,
                                                   value)
        d = self.get(sql)
        return d

    def table_insert(self, table_name, item):
        '''item is a dict : key is mysql table field'''
        fields = list(item.keys())
        values = list(item.values())
        fieldstr = ','.join(fields)
        valstr = ','.join(['%s'] * len(item))
        for i in range(len(values)):
            if isinstance(values[i], str):
                values[i] = values[i].encode('utf8')
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fieldstr, valstr)
        try:
            last_id = self.execute(sql, *values)
            return last_id
        except Exception as e:
            if e.args[0] == 1062:
                # just skip duplicated item
                pass
            else:
                traceback.print_exc()
                print('sql:', sql)
                print('item:')
                for i in range(len(fields)):
                    vs = str(values[i])
                    if len(vs) > 300:
                        print(fields[i], ' : ', len(vs), type(values[i]))
                    else:
                        print(fields[i], ' : ', vs, type(values[i]))
                raise e

    def table_update(self, table_name, updates, field_where, value_where):
        '''updates is a dict of {field_update:value_update}'''
        upsets = []
        values = []
        for k, v in updates.items():
            s = '%s=%%s' % k
            upsets.append(s)
            values.append(v)
        upsets = ','.join(upsets)
        sql = 'UPDATE %s SET %s WHERE %s="%s"' % (
            table_name,
            upsets,
            field_where,
            value_where,
        )
        self.execute(sql, *(values))


if __name__ == '__main__':
    # db = Connection('localhost', 'db_name', 'user', 'password')
    # 获取一条记录
    # sql = 'select * from test_table where id=%s'
    # data = db.get(sql, 2)

    # # 获取多天记录
    # sql = 'select * from test_table where id>%s'
    # data = db.query(sql, 2)

    # # 插入一条数据
    # sql = 'insert into test_table(title, url) values(%s, %s)'
    # last_id = db.execute(sql, 'test', 'http://a.com/')
    # # 或者
    # last_id = db.insert(sql, 'test', 'http://a.com/')

    # 使用更高级的方法插入一条数据
    # item = {
    #     'title': 'test',
    #     'url': 'http://a.com/',
    # }
    # last_id = db.table_insert('test_table', item)

    # 连接数据库
    connection = Connection()
    mongo = Connection().db
    db = mongo[connection.database]
    print('database list: ', mongo.list_database_names())  # 查看数据库列表

    # 集合管理
    collection = db[connection.collection]
    print('collection list: ', db.list_collection_names())  # 查看集合列表
    # collection.drop()  # 删除集合

    # 添加一条记录
    document = {"name": "Sancho", "age": 23}
    ret = collection.insert_one(document)
    # 添加多条记录
    document_list = [{"name": "Sancho", "age": 23}, {"name": "Leo", "age": 27}]
    ret = collection.insert_many(document_list)

    # 删除一条记录
    # query = {"name": "Leo"}
    # ret = collection.delete_one(query)
    # or
    # query = {"_id": ObjectId("60d925e127bd4b7769251002")}
    # ret = collection.delete_one(query)
    # 删除多条记录
    # query = {"age": {"$gt": "23"}}
    # ret = collection.delete_many(query)
    # or
    # ret = collection.delete_many(query)

    # 更新文档
    query = {"name": "Sancho"}
    collection.update_one(query, {"$set": {"age": 18}})
    # 更新所有文档
    # query = {"age": {"$gt": "18"}}
    # collection.update_many(query, {"$set": {"age": 18}})

    # 查询文档
    print(collection.find_one())  # 查询一条
    print([collection for collection in collection.find()])  # 查询所有文档
    # print(collection.find().limit(3))  # 限制查询结果的数量，返回cursor
    # print(collection.find({"$where": "this.age==18"}))  # 自定义条件函数，返回cursor
