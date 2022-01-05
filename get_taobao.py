import datetime
from typing import Text
import requests as rq
from lxml import etree
import pandas as pd
"""
1.访问网站拿到页面数据
2.解析网页整理信息
3.输出信息
"""


#读取页面
def access_web(url, params, headers):
    rsp = rq.get(url=url, params=params, headers=headers)
    rsp.encoding = "utf-8"
    print(rsp)
    return rsp.text


#整理数据
# def get_data(rsp):
#     data = []
#     html = etree.HTML(rsp)
#     divs = html.xpath('//*[@id="mainsrp-itemlist"]/div/div/div[1]')
#     for div in divs:

#         data.append(div.xpath('//div[1]/div[2]/div[2]/a/text()'))

#     print(data)

if __name__ == "__main__":
    today = str(datetime.date.today()).replace("-", "")
    terms = "连衣裙"  #input("请输入搜索词：")
    url = "https://s.taobao.com/search"
    cookie = r"thw=cn; hng=CN%7Czh-CN%7CCNY%7C156; t=10a240005ef5e6b55fe1e982c549dccd; enc=j8UsIQEpa7frZIpbyVYajPG6i2l%2BilhuKgEaLz2yIruOi5y%2BBuaFvSJustRmMIKItMrKYh0dzJ3wIThlYbli2Q%3D%3D; _m_h5_tk=ff4d2daecf7503382be99b540a3be702_1641396526659; _m_h5_tk_enc=2b21ec6d3fbb784575bb1be0e1f2bf6e; cookie2=10bde32a8306551b582c65dc038d7dee; _tb_token_=ebe7eb39e0f13; xlly_s=1; alitrackid=www.taobao.com; _samesite_flag_=true; cna=blzQGct14WMCAd7UevOKck6H; _uab_collina=164138960777420501378615; x5sec=7b227365617263686170703b32223a226131303364343833663164383262333562666434313531643030306334333562434f2b37316f344745495478714d6d6c7135765944786f4d4d6a6b344d4459774d54557a4e6a73784d4b6546677037382f2f2f2f2f77453d227d; sgcookie=E100MweeR31q2sCHXr9R281WY9CH3EhFRunJJbtvIDxa483CgB8aNTfzYcvMivPWLFqG33Hq5k0%2Bt6QRi06K9D0tWfIKLB0oxZCDAK9z76sOm%2Ffx6lWm%2BLD3fBjZrJrBMzMz; unb=2980601536; uc3=vt3=F8dCvUs0si7%2B4vUyaWg%3D&lg2=URm48syIIVrSKA%3D%3D&id2=UUGq0U77Orn3vQ%3D%3D&nk2=CtWCLLWc1edoveRIgpA0FQ%3D%3D; csg=7b096a5e; lgc=is%5Cu5979%5Cu5E26%5Cu7740%5Cu6211%5Cu79BB%5Cu5F00%5Cu4E36; cancelledSubSites=empty; cookie17=UUGq0U77Orn3vQ%3D%3D; dnk=is%5Cu5979%5Cu5E26%5Cu7740%5Cu6211%5Cu79BB%5Cu5F00%5Cu4E36; skt=887536fb30b1322c; existShop=MTY0MTM4OTYwNg%3D%3D; uc4=id4=0%40U2OdJY2HtWPhY%2BP375zl3nt%2FeWOm&nk4=0%40CNRxUNLslA0SksE4B0iqJdiabGeifEG7V8SX; tracknick=is%5Cu5979%5Cu5E26%5Cu7740%5Cu6211%5Cu79BB%5Cu5F00%5Cu4E36; _cc_=Vq8l%2BKCLiw%3D%3D; _l_g_=Ug%3D%3D; sg=%E4%B8%B667; _nk_=is%5Cu5979%5Cu5E26%5Cu7740%5Cu6211%5Cu79BB%5Cu5F00%5Cu4E36; cookie1=BxBB3Ai0evb6FZjhWJ39%2BkSY6MVxcPBfQ5L4HpTiuLA%3D; lastalitrackid=login.taobao.com; mt=ci=21_1; uc1=cookie15=Vq8l%2BKCLz3%2F65A%3D%3D&cookie21=Vq8l%2BKCLivbdjeuVIQ2NTQ%3D%3D&cookie14=UoewAeY5zi9UQQ%3D%3D&pas=0&existShop=false&cookie16=UIHiLt3xCS3yM2h4eKHS9lpEOw%3D%3D; JSESSIONID=DA66EBB1D6009C057816F09612105D4C; isg=BCkpNk7kbI1p4FCltZHm-WwjONWD9h0ochyCXMseEpBPkkikE0MI-F7EVDakCrVg; tfstk=czYPBbxgY43rtr7IXa_EABXSCcsRZSdktr51q3tBcPjbETjliM2diT1Rg6N4Kgf..; l=eBIdt-fugFatqr9ABO5anurza77t2IRb8sPzaNbMiInca6gdtFaywNCpF5YkSdtjgtffHetP6yJJoRewrM4dg2HvCbKrCyCkBxJ6-"
    params = {
        "p": terms,
        "imgfile": "",
        "js": "1",
        "stats_click": "",
        "search_radio_all": "1",
        "initiative_id": "staobaoz_" + today,
        "ie": "utf8",
    }
    headers = {
        "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "cookie": cookie,
        "referer": url
    }
    rsp = access_web(url=url, params=params, headers=headers)
