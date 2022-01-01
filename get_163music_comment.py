#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests as rq
import re
from Crypto.Cipher import AES
from base64 import b64encode
import json
"""
可获取到网易云音乐歌曲页面的评论信息
1.获取传递参数处理成密钥
2.模拟加密过程(二次加密)
"""


#获取加密参数
def get_key():
    data = {
        "csrf_token": "",
        "cursor": "-1",
        "offset": "0",
        "orderType": "1",
        "pageNo": "1",
        "pageSize": "20",
        "rid": "R_SO_4_93948",
        "threadId": "R_SO_4_93948"
    }
    d = json.dumps(data)
    g = "0CoJUm6Qyw8W8jud"
    i = "QdpZ1kaILNg5xQxY"
    # e = '010001'
    # f = "00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7"
    encSecKey = "14d7034ebe109693de5b829997cf59dfc2fe429041e8d8d978bfc1ff0002995d66de6cf2fcc7fb024d70b9be58155a8ec829900d22ae25357ec84baa0af3564c58efbc300f94659335afb8f697961a249ff6ebec3e469b4ef6beed923a642eea085e02d121727d43951458664d9f00c469ee0657d1d246320dbf7e36ddd921ac"
    return deciphering(deciphering(d, g), i), encSecKey


#模仿加密过程
def deciphering(a, b):
    iv = "0102030405060708"
    aes = AES.new(key=b.encode("utf-8"),
                  IV=iv.encode("utf-8"),
                  mode=AES.MODE_CBC)
    bs = aes.encrypt(to_16(a).encode("utf-8"))
    return str(b64encode(bs), "utf-8")


#处理密文（AES独特加密方式参数处理）
def to_16(num):
    pad = 16 - len(num) % 16
    num += chr(pad) * pad
    return num


#抓取
if __name__ == "__main__":
    url = "https://music.163.com/weapi/comment/resource/comments/get"
    headers = {
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    }
    params = {"csrf_token": ""}
    #写入加密参数，获取密钥
    encText, encSecKey = get_key()
    # 通过连接和密钥访问页面数据
    data = {"params": encText, "encSecKey": encSecKey}
    rsp = rq.post(url=url, headers=headers, params=params, data=data)  #获取歌曲页面
    # 解析页面数据得到评论
    # print(rsp.text)
    html = re.compile(
        r'content":"(?P<content>.*?)",".*?nickname":"(?P<name>.*?)","a'
    )  #正则匹配评论
    iter = html.finditer(rsp.text, re.S)
    for i in iter:
        print(i.group("name"), " : ", i.group("content"))
