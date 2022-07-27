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
        # self.close()
        # self._db = pymysql.connect(**self._db_args)
        # self._db.autocommit(True) # 二次确认

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

    def get_all(self, query: dict = None, limit=None, _fn=False) -> list:
        """返回所有数据"""
        if query == None:
            query = {}
        self._cursor(query)
        # FIXME:应该传参limit到_cursor
        if limit:
            self.cursor = self.cursor.limit(limit)
        if _fn:
            return self.cursor  # 如果是函数访问则返回游标数据
        # FIXME:应该放到游标迭代托管里
        return [i for i in self.cursor]

    # def _ensure_connected(self):
    #     # Mysql by default closes client connections that are idle for
    #     # 8 hours, but the client library does not report this fact until
    #     # you try to perform a query and it fails.  Protect against this
    #     # case by preemptively closing and reopening the connection
    #     # if it has been idle for too long (7 hours by default).
    #     if (self._db is None
    #             or (time.time() - self._last_use_time > self.max_idle_time)):
    #         self.reconnect()
    #     self._last_use_time = time.time()

    def _ensure_connected(self):
        # FIXME:
        """ 
        Mysql by default closes client connections that are idle for
        8 hours, but the client library does not report this fact until
        you try to perform a query and it fails.  Protect against this
        case by preemptively closing and reopening the connection
        if it has been idle for too long (7 hours by default).
        """
        if (self.db is None
                or (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    # def _cursor(self):
    #     self._ensure_connected()
    #     self._db.cursor()

    def _find(self, query):
        self.cursor = self.db_database_collection.find(query,
                                                       no_cursor_timeout=True)
        return

    def _cursor(self, query):
        """处理游标（超时问题）"""
        # FIXME:https://www.mongodb.com/docs/v5.0/reference/method/cursor.noCursorTimeout/
        self._ensure_connected()
        try:
            self._find(query)
            self.refreshTimestamp = time.time()
        except pymongo.errors.CursorNotFound:
            self.cursor.close()
            self._cursor()

        return

    # def execute(self, query, *parameters, **kwparameters):
    #     """Executes the given query, returning the lastrowid from the query."""
    #     cursor = self._cursor()
    #     try:
    #         cursor.execute(query, kwparameters or parameters)
    #         return cursor.lastrowid
    #     except Exception as e:
    #         if e.args[0] == 1062:
    #             pass
    #         else:
    #             traceback.print_exc()
    #             raise e
    #     finally:
    #         cursor.close()

    # insert = execute

    # def get(self, query, *parameters, **kwparameters):
    #     """返回查询结果"""
    #     cursor = self._cursor()
    #     try:
    #         cursor.execute(query, kwparameters or parameters)
    #         return cursor.fetchone()
    #     finally:
    #         cursor.close()

    def get(self, query: dict = None, limit=None):
        """返回查询结果的一条"""
        if self.cursor != None:
            if self.query != query:  # 已有游标，重新获取新游标
                self.query = query
                self.get_all(self.query, _fn=True)
            return self.cursor.next()  # 已有游标，获取下一个值
        self.query = query
        self.get_all(query, _fn=True)  # 没有游标，创建新游标
        return self.cursor.next()

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

    ## =============== high level method for table ===================

    # def table_has(self, table_name, field, value):
    #     if isinstance(value, str):
    #         value = value.encode('utf8')
    #     sql = 'SELECT %s FROM %s WHERE %s="%s"' % (field, table_name, field,
    #                                                value)
    #     d = self.get(sql)
    #     return d

    # def table_insert(self, table_name, item):
    #     '''item is a dict : key is mysql table field'''
    #     fields = list(item.keys())
    #     values = list(item.values())
    #     fieldstr = ','.join(fields)
    #     valstr = ','.join(['%s'] * len(item))
    #     for i in range(len(values)):
    #         if isinstance(values[i], str):
    #             values[i] = values[i].encode('utf8')
    #     sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fieldstr, valstr)
    #     try:
    #         last_id = self.execute(sql, *values)
    #         return last_id
    #     except Exception as e:
    #         if e.args[0] == 1062:
    #             # just skip duplicated item
    #             pass
    #         else:
    #             traceback.print_exc()
    #             print('sql:', sql)
    #             print('item:')
    #             for i in range(len(fields)):
    #                 vs = str(values[i])
    #                 if len(vs) > 300:
    #                     print(fields[i], ' : ', len(vs), type(values[i]))
    #                 else:
    #                     print(fields[i], ' : ', vs, type(values[i]))
    #             raise e

    # def table_update(self, table_name, updates, field_where, value_where):
    #     '''updates is a dict of {field_update:value_update}'''
    #     upsets = []
    #     values = []
    #     for k, v in updates.items():
    #         s = '%s=%%s' % k
    #         upsets.append(s)
    #         values.append(v)
    #     upsets = ','.join(upsets)
    #     sql = 'UPDATE %s SET %s WHERE %s="%s"' % (
    #         table_name,
    #         upsets,
    #         field_where,
    #         value_where,
    #     )
    #     self.execute(sql, *(values))

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
    print(connection.get())  # 返回查询结果中的一条
    print(connection.get())  # 继续查询下一条

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