#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: urldb.py
#CREATE_TIME: 2022-07-25
#AUTHOR: Sancho
"""
设置或查询数据库中的链接状态
"""


class UrlDB:
    """
    使用数据库储存已完成的url
    """
    status_failure = b'0'
    status_success = b'1'

    def __init__(self, db):
        self.db = db

    def set_success(self, url):
        """添加成功的数据"""
        if isinstance(url, str):  # 判断数据是否是字符串
            url = url.encode('utf8')
        try:
            self.db.insert({
                "url": url,
                "status": self.status_success
            })  # 尝试写入数据
            print("success url to db:%s" % url)
            s = True
        except:
            s = False
        return s

    def set_failure(self, url):
        """添加失败的数据"""
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            self.db.insert({"url": url, "status": self.status_failure})
            print("failure url to db:%s" % url)
            s = True
        except:
            s = False
        return s

    def has(self, url):
        """判断数据是否已存在数据库"""
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            # attr = [i for i in self.db.get_one({"url": url})]  
            attr = self.db.get_one({"url": url}) # 查询指定条件的数据
            return attr
        except:
            pass
        return False