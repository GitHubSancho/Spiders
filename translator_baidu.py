#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests as rq

if __name__ == "__main__":
    kw = input("enter a word:")#输入单词
    params = {"kw": kw}#接收输入
    headers = {
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    }#标识
    url = "https://fanyi.baidu.com/sug"#翻译接口
    rsp = rq.post(url=url, data=params)#发送请求
    print(rsp.json())#输出json
    rsp.close()
    print("over!")
