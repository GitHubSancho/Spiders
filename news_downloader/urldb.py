#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: urldb.py
#CREATE_TIME: 2022-07-25
#AUTHOR: Sancho
"""
数据库相关操作
"""

# import pymongo
import ezpymongo


class UrlDB:
    """
    使用数据库储存已完成的url
    """
    status_failure = b'0'
    status_success = b'1'

    # def __init__(self, db_name, host, port):
    #     self.client = pymongo.MongoClient("mongodb://{}:{}/".format(
    #         host, port))  # 连接数据库
    #     self.db = self.client[db_name]
    #     # self.myset = self.db[set_name]

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
        self.db = ezpymongo.Connection(host=host,
                                       database=database,
                                       collection=collection,
                                       user=user,
                                       password=password,
                                       port=port,
                                       max_idle_time=max_idle_time,
                                       timeout=timeout,
                                       time_zone=time_zone)

    # def set_success(self, host, url):
    #     """添加成功的数据"""
    #     if isinstance(url, str):  # 判断数据是否是字符串
    #         url = url.encode('utf8')
    #     try:
    #         self.db[host].insert_one({
    #             "url": url,
    #             "status": self.status_success
    #         })  # 尝试写入数据
    #         s = True
    #     except:
    #         s = False
    #     return s

    def set_success(self, url):
        """添加成功的数据"""
        if isinstance(url, str):  # 判断数据是否是字符串
            url = url.encode('utf8')
        try:
            self.db.insert({
                "url": url,
                "status": self.status_success
            })  # 尝试写入数据
            s = True
        except:
            s = False
        return s

    # def set_failure(self, host, url):
    #     """添加失败的数据"""
    #     if isinstance(url, str):
    #         url = url.encode('utf8')
    #     try:
    #         self.db[host].insert_one({
    #             "url": url,
    #             "status": self.status_failure
    #         })
    #         s = True
    #     except:
    #         s = False
    #     return s

    def set_failure(self, url):
        """添加失败的数据"""
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            self.db.insert({"url": url, "status": self.status_failure})
            s = True
        except:
            s = False
        return s

    # def has(self, host, url):
    #     """判断数据是否已存在数据库"""
    #     if isinstance(url, str):
    #         url = url.encode('utf8')
    #     try:
    #         attr = [i for i in self.db[host].find({"url": url})]  # 查询指定条件的数据
    #         return attr
    #     except:
    #         pass
    #     return False

    def has(self, url):
        """判断数据是否已存在数据库"""
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            attr = [i for i in self.db.get_one({"url": url})]  # 查询指定条件的数据
            return attr
        except:
            pass
        return False


if __name__ == "__main__":
    # db = UrlDB("demo", "127.0.0.1", "27017")
    # print(db)
    db = UrlDB()
    print(db)