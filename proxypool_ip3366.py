#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests as rq
from lxml import etree
import telnetlib as tn
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor as tpe


#提取IP
def draw(url, i):
    # 解析网页
    html = rq.get(url=url, params={"action": "china", "page": i})
    html.encoding = "utf-8"
    rsp = html.text
    html.close()

    # 提取网页数据
    tbodys = etree.HTML(rsp).xpath(
        "/html/body/section/section/div[2]/table/tbody")
    ips = [
        pd.DataFrame([
            # 提取网页内ip
            tbody.xpath("./tr/td[1]/text()"),
            tbody.xpath("./tr/td[2]/text()"),
            tbody.xpath("./tr/td[3]/text()"),
            tbody.xpath("./tr/td[4]/text()"),
            tbody.xpath("./tr/td[5]/text()")
        ],
                     index=["ip", "port", "anmt", "http", "place"]).T # 设置表头和倒置
        for tbody in tbodys
    ][0].drop_duplicates()# 删除重复

    ips["ping"] = ips.apply(ping, axis=1)
    return ips


# 验证IP
def ping(self):
    try:
        stime = time.time()
        test = tn.Telnet(self["ip"], port=self["port"], timeout=5)#ping
        etime = time.time()
        print(self["ip"], self["port"], "YES")
        test.close()
        return etime - stime
    except:
        print(self["ip"], self["port"], "NO")
        return -1


if __name__ == "__main__":
    url = "https://proxy.ip3366.net/free/"

    # 使用线程池
    ips = []
    with tpe(10) as pool:
        for i in range(1, 11):
            ips.append(pool.submit(draw, url, i))
    ips = pd.concat([i.result() for i in ips], axis=0,
                    ignore_index=True).drop_duplicates()

    # 排序和筛选
    ips.sort_values(by="ping", inplace=True, ascending=True)
    ips_ture = ips[ips["ping"] != -1]

    # 设置输出
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    print(ips_ture.reset_index(drop=True))
    ips_ture.to_csv("proxypool.csv", index=False)  #输出到文件
