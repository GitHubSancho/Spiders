from asyncio import tasks
import requests as rq
import re
import asyncio, aiohttp, aiofiles
from lxml import etree
from Crypto.Cipher import AES
import os
"""
主要功能：下载电影

1.获取电影播放页面代码，找到第一层m3u8文件位置，和电影名
    #https://vod1.bdzybf1.com/20200824/dYqwJfm0/index.m3u8
2.读取m3u8文件，拿到第一层m3u8文件域名+第二层m3u8地址后缀合并成完整链接
    #https://vod1.bdzybf1.com/ + /20200824/dYqwJfm0/1000kb/hls/index.m3u8
3.根据第二层m3u8完整地址获取ts链接列表和密钥链接
    #https://ts1.yuyuangewh.com:9999/20200824/dYqwJfm0/1000kb/hls/icGs5jKF.ts
4.根据ts下载链接列表异步下载
    #download ts_list
5.根据第二层m3u8文件中的解密方式，获取密钥
    ##EXT-X-KEY:METHOD=AES-128,URI="https://ts1.yuyuangewh.com:9999/20200824/dYqwJfm0/1000kb/hls/key.key"
6.根据密钥，异步解密ts文件
    #key.key --> icGs5jKF.ts...ts_file
7.使用os命令合并ts文件
    #copy \\b 1.ts+2.ts+...x.ts
"""


#传递电影页面url和头部信息，获取页面代码，解析并返回第一层m3u8文件位置和电影名
def get_first_m3u8_url(url, headers):
    with rq.get(url, headers) as rsp:
        rsp.encoding = "utf-8"  #转码
        obj = re.compile(r'"http(?P<first_m3u8>.*?)\.m3u8"')  #设置匹配条件
        first_m3u8_url = [
            i.groups("first_m3u8") for i in obj.finditer(rsp.text)
        ][0][0]  #得到匹配的第一条链接
        name = etree.HTML(
            rsp.text).xpath("/html/head/title/text()")[0].split(" ")[0]  #获取电影名

    return "http" + first_m3u8_url + ".m3u8", name  #填补链接信息并返回


#传递第一层m3u8文件域名、地址和头部信息，找到第二层m3u8文件位置后缀并拼接域名成完整链接
def get_second_m3u8_url(domains, url, headers):
    with rq.get(url, headers) as rsp:
        rsp.encoding = "utf-8"  #转码
        second_m3u8_url = rsp.text.split("\n")[-2]  #提取文本中的m3u8的地址后缀，在倒数第二个

    return domains + second_m3u8_url  #合并域名和地址后缀并返回


#根据第二层m3u8文件筛选ts下载列表，并返回ts链接列表和密钥获取方式
def get_m3u8_file(url, headers):
    with rq.get(url, headers) as rsp:
        rsp.encoding = "utf-8"  #转码
        #返回提取ts链接列表，文本以换行符切割后循环，如果切片文本开头不为#号则删除空白符输出
        ts_list = [
            i.strip() for i in rsp.text.split("\n") if not i.startswith("#")
        ]
        key_url = re.compile(r'URI="(?P<key_url>.*?)"').search(rsp.text).group(
            1)  #获取密钥链接

    return ts_list[:-1], key_url  #返回ts链接列表，删除最后一个空白链接元素


#异步下载ts文件
async def download_ts(ts_url, name, session, path_ts):
    async with session.get(ts_url) as rsp:
        async with aiofiles.open(f"{path_ts}/{name}", mode="wb") as f:
            await f.write(await rsp.content.read())


#创建异步下载任务
async def aio_download(ts_list, path_ts):
    tasks = []
    # 预创建异步网络获取任务
    # async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(
    #     limit=400)) as session:  #limit控制最大连接数
    async with aiohttp.ClientSession() as session:
        for i, url in zip(range(len(ts_list)), ts_list):
            name = str(i).zfill(9) + ".ts"  #取url后缀文件名
            task = asyncio.create_task(download_ts(url, name, session,
                                                   path_ts))
            tasks.append(task)
        await asyncio.wait(tasks)


#获取密钥
def get_key(url, headers):
    with rq.get(url, headers) as rsp:
        return rsp.text.encode('utf-8')  #将字符串转为


#异步解密文件
async def dec_ts(key, name, path_ts, path_temp):
    aes = AES.new(key=key, IV=b"0000000000000000",
                  mode=AES.MODE_CBC)  #偏移量默认为密钥位数个0,解密方式需要自行尝试默认CBC
    async with aiofiles.open(f"{path_ts}/{name}", mode="rb") as f1:  #读取ts文件
        async with aiofiles.open(f"{path_temp}/{name}", mode="wb") as f2:  #
            bs = await f1.read()
            await f2.write(aes.decrypt(bs))  #写解密后的文件


#创建异步解密任务
async def aio_dec(key, ts_list, path_ts, path_temp):
    tasks = []
    for i in range(len(ts_list)):  #读取ts_list文件名列表
        name = str(i).zfill(9) + ".ts"
        task = asyncio.create_task(dec_ts(key, name, path_ts, path_temp))
        tasks.append(task)
    await asyncio.wait(tasks)


#合并解密后的文件
def merge_ts(path_temp, name):
    #windows
    os.system(f'copy /B {path_temp}\*.ts {name}.mp4')
    # print(f'copy {path_temp}/*.ts {name}.mp4')


if __name__ == '__main__':
    url = "https://qiletv.net/play/69931-0-0.html"  #输入域名
    headers = {
        "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    }  #设置访问标识

    #1.找到第一层m3u8地址和电影名
    first_m3u8_url, name = get_first_m3u8_url(url, headers)

    #2.找到第二层m3u8地址
    domains = first_m3u8_url.split(".com")[0] + ".com"  #找到第一层m3u8的域名
    second_m3u8_url = get_second_m3u8_url(domains, first_m3u8_url, headers)

    #3.拿到ts链接，名字列表和密钥获取链接
    ts_list, key_url = get_m3u8_file(second_m3u8_url, headers)

    #4.异步下载ts文件
    print("正在下载ts文件，请稍等...")
    ts_path = ".\\video_ts"
    os.makedirs(ts_path) if not os.path.exists(ts_path) else print(
        f"将下载到：{ts_path}")
    asyncio.get_event_loop().run_until_complete(aio_download(ts_list, ts_path))
    print("下载完毕！")

    # #5.获取密钥
    key = get_key(key_url, headers)

    #6.解密ts文件
    temp_path = ".\\video_temp"
    os.makedirs(temp_path) if not os.path.exists(temp_path) else print(
        f"将下载到：{temp_path}")
    asyncio.run(aio_dec(key, ts_list, ts_path, temp_path))
    print("解密完毕！")

    # #7.合并解密后的文件
    merge_ts(temp_path, name)
    print("已全部完成!")
