#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests as rq

if __name__ == "__main__":
    kw = input("enter a word:")
    params = {"kw": kw}
    headers = {
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    }
    url = "https://fanyi.baidu.com/sug"
    rsp = rq.post(url=url, data=params)
    print(rsp.json())
    rsp.close()
    print("over!")
