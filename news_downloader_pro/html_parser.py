#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: html_parser.py
#CREATE_TIME: 2022-08-08
#AUTHOR: Sancho

import urllib.parse as urlparse
import aiohttp
import url_downloader


def _filter_good(urls, url_pool):
    goodlinks = []
    for url in urls:
        host = urlparse.urlparse(url).netloc
        hub_hosts = [urlparse.urlparse(hub).netloc for hub in url_pool.hubs]
        if host in hub_hosts:
            goodlinks.append(url)
    return goodlinks


def process(session, url, ishub, url_pool, db):
    status, html, redirected_url = url_downloader.fetch(session, url)
    # await url_pool.set_status(url, status)
    url_pool.set_status(url, status)
    if redirected_url != url:
        url_pool.set_status(redirected_url, status)
    # 提取hub网页中的链接, 新闻网页中也有“相关新闻”的链接，按需提取
    if status != 200:
        return
    if ishub:
        newlinks = url_downloader.extract_links_re(redirected_url, html)
        goodlinks = _filter_good(newlinks, url_pool)
        print(f"{len(goodlinks)}/{len(newlinks)}, goodlinks/newlinks")
        url_pool.addmany(goodlinks)
    else:
        db.save_to_db(redirected_url, html)
