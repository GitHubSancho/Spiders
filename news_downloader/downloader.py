#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: downloader.py
#CREATE_TIME: 2022-07-14
#AUTHOR: Sancho
"""
下载器
"""

import requests
import cchardet
import traceback


def downloader(url, timeout=10, headers=None, debug=False, binary=False):
    _headers = {
        'User-Agent':
        ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
         ),
    }
    if headers:  # 判断ua是否传递，没有则选择上面ua
        _headers = headers

    redirected_url = url

    try:
        # 尝试访问网页
        resp = requests.get(url, headers=_headers, timeout=timeout)
        if binary:  # 是否需要二进制文件
            html = resp.content
        else:
            html = resp.content.decode(
                cchardet.detect(resp.content)['encoding'])  # 识别并改变编码

        status = resp.status_code
        redirected_url = resp.url  # 重定向url

    except:
        # 错误检测
        if debug:
            traceback.print_exc()  # 打印错误行
        msg = 'failed download: {}'.format(url)
        print(msg)

        if binary:
            html = b''
        else:
            html = ''
        status = 0
    return status, html, redirected_url


if __name__ == '__main__':
    url = 'http://news.baidu.com/'
    s_code, html, r_url = downloader(url)
    print(s_code, len(html), r_url)