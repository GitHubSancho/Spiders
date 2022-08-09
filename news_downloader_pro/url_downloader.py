#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: downloader.py
#CREATE_TIME: 2022-07-14
#AUTHOR: Sancho
"""
下载器
"""

import re
import urllib.parse as urlparse
import requests
import cchardet
import traceback
import logging

# def downloader(url, timeout=10, headers=None, debug=False, binary=False):
#     _headers = {
#         'User-Agent':
#         ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
#          ),
#     }
#     if headers:  # 判断ua是否传递，没有则选择上面ua
#         _headers = headers

#     redirected_url = url

#     try:
#         # 尝试访问网页
#         print("donwnload:%s" % url)
#         resp = requests.get(url, headers=_headers, timeout=timeout)
#         if binary:  # 是否需要二进制文件
#             html = resp.content
#         else:
#             html = resp.content.decode(cchardet.detect(
#                 resp.content)['encoding'],
#                                        errors='ignore')  # 识别并改变编码

#         status = resp.status_code
#         redirected_url = resp.url  # 重定向url
#     except:
#         # 错误检测
#         if debug:
#             traceback.print_exc()  # 打印错误行
#         msg = 'failed download: {}'.format(url)
#         print(msg)

#         if binary:
#             html = b''
#         else:
#             html = ''
#         status = 0
#     return status, html, redirected_url

G_BIN_POSTFIX = ('exe', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'pdf',
                 'jpg', 'png', 'bmp', 'jpeg', 'gif', 'zip', 'rar', 'tar',
                 'bz2', '7z', 'gz', 'flv', 'mp4', 'avi', 'wmv', 'mkv', 'apk')

G_NEWS_POSTFIX = ('.html?', '.htm?', '.shtml?', '.shtm?')


def clean_url(url):
    # 1. 是否为合法的http url
    if not url.startswith('http'):
        return ''
    # 2. 去掉静态化url后面的参数
    for np in G_NEWS_POSTFIX:
        p = url.find(np)
        if p > -1:
            p = url.find('?')
            url = url[:p]
            return url
    # 3. 不下载二进制类内容的链接
    up = urlparse.urlparse(url)
    path = up.path
    if not path:
        path = '/'
    postfix = path.split('.')[-1].lower()
    if postfix in G_BIN_POSTFIX:
        return ''

    # 4. 去掉标识流量来源的参数
    # badquery = ['spm', 'utm_source', 'utm_source', 'utm_medium', 'utm_campaign']
    good_queries = []
    for query in up.query.split('&'):
        qv = query.split('=')
        if qv[0].startswith('spm') or qv[0].startswith('utm_'):
            continue
        if len(qv) == 1:
            continue
        good_queries.append(query)
    query = '&'.join(good_queries)
    url = urlparse.urlunparse((
        up.scheme,
        up.netloc,
        path,
        up.params,
        query,
        ''  #  crawler do not care fragment
    ))
    return url


g_pattern_tag_a = re.compile(r'<a[^>]*?href=[\'"]?([^> \'"]+)[^>]*?>(.*?)</a>',
                             re.I | re.S | re.M)


def extract_links_re(url, html):
    """使用re模块从hub页面提取链接"""
    print(f"extract:{url}")
    newlinks = set()
    aa = g_pattern_tag_a.findall(html)
    for a in aa:
        link = a[0].strip()
        if not link:
            continue
        link = urlparse.urljoin(url, link)
        if link := clean_url(link):
            newlinks.add(link)
    print("add:%d urls" % len(newlinks))
    return newlinks


def fetch(session, url, headers=None, timeout=9, binary=False):
    """异步"""
    _headers = headers or {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }

    try:
        resp = session.get(url, headers=_headers, timeout=timeout)
        status = resp.status_code
        session.encoding = "utf-8"
        html = resp.text
        redirected_url = str(resp.url)
    except Exception as e:
        msg = f'Failed download: {url} | exception: {str(type(e))}, {str(e)}'
        print(msg)
        html = ''
        status = 0
        redirected_url = url
    return status, html, redirected_url
