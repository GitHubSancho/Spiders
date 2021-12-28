#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests as rq
from lxml import etree
import telnetlib as tn
import pandas as pd
import time

if __name__ == "__main__":
    #提取IP
    url = "https://proxy.ip3366.net/free/"
    rsp = []
    for i in range(1, 11):
        html = rq.get(url=url, params={"action": "china", "page": i})
        html.encoding = "utf-8"
        rsp.append(html.text)
        html.close()

    tbodys = [
        etree.HTML(i).xpath("/html/body/section/section/div[2]/table/tbody")
        for i in rsp
    ]

    ips = pd.concat([
        pd.DataFrame([
            body.xpath("./tr/td[1]/text()"),
            body.xpath("./tr/td[2]/text()"),
            body.xpath("./tr/td[3]/text()"),
            body.xpath("./tr/td[4]/text()"),
            body.xpath("./tr/td[5]/text()")
        ],
                     index=["ip", "port", "anmt", "http", "place"]).T
        for tbody in tbodys for body in tbody
    ],
                    axis=0,
                    ignore_index=True).drop_duplicates().reset_index(drop=True)

    #测试IP
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)

    def ping(self):
        try:
            stime = time.time()
            test = tn.Telnet(self["ip"], port=self["port"], timeout=5)
            etime = time.time()
            print(self["ip"], self["port"], "YES")
            test.close()
            return etime - stime
        except:
            print(self["ip"], self["port"], "NO")
            return -1

    ips["ping"] = ips.apply(ping, axis=1)
    ips.sort_values(by="ping", inplace=True, ascending=True)
    ips_ture = ips[ips["ping"] != -1]
    print(ips_ture)
    ips_ture.to_csv("proxypool.csv", index=False)
