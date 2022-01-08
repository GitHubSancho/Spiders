#!/usr/bin/python
# -*- coding: utf-8 -*-

import asyncio
import time
import aiohttp
import aiofiles
import requests
from lxml import etree


#读取页面链接
def get_urls(index):
    with requests.get(index) as rsp:  #读取网页
        html = etree.HTML(rsp.text)
        links = html.xpath("/html/body/div[2]/div[8]/ul/li/a/@href")  #定位图片链接
    return links


#为每个url创建异步任务
async def main(foo, urls):
    tasks = []
    for url in urls:
        tasks.append(foo(url))
    await asyncio.wait(tasks)


#异步爬取下载链接
async def get_srcs(links):
    global site
    url = "https://www.umei.cc" + links  #拼接链接
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as rsp:
            html = etree.HTML(await rsp.content.read())
            site.append(html.xpath("/html/body/div[2]/div[10]/p/img/@src")[0])
    #         # async with aiofiles.open("imgs/" + name, mode="wb") as f:
    #         #     await f.write(await rsp.content.read())


#异步下载图片
async def download_pic(site):
    name = site.rsplit("/", 1)[1]  #切割链接，选取最后一部分作为文件名
    async with aiohttp.ClientSession() as session:
        async with session.get(site) as rsp:
            async with aiofiles.open("imgs/" + name, mode="wb") as f:
                await f.write(await rsp.content.read())
    print(name, "OK!")


if __name__ == "__main__":
    site = []
    star = time.time()
    index = "https://www.umei.cc/weimeitupian/"  #想爬的首页
    links = get_urls(index)  #获取首页内图片的链接
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(get_srcs, links))  #进入大图页面寻找图片地址
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(download_pic, site))  #下载图片
    print(time.time() - star)
