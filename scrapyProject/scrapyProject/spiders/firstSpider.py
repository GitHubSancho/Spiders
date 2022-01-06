import scrapy
import json
from scrapyProject.items import ScrapyprojectItem


class FirstspiderSpider(scrapy.Spider):
    # 爬虫唯一标识
    name = 'firstSpider'
    # 允许的域名（网站可能会访问其它域名，所以一般不使用）
    # allowed_domains = ['www.baidu.com']
    # 自动发送请求的url列表
    start_urls = ['https://www.thepaper.cn/consumer_complaint_data.jsp'
                  ]  #?categoryId=0&pageidx=2

    # 数据解析（response是请求成功后的响应对象）
    def parse(self, response):
        # xpath返回列表，列表中元素是Selector类型对象
        # extract()对Selector类型对象提取，列表为全部提取
        # extract_first()返回提取到的第一个对象
        html = json.loads(response.text)["contList"]
        # print(html)
        for i in html:
            item = ScrapyprojectItem()
            item["name"] = i['userInfo']["name"]
            item["title"] = i["title"]
            item["complaintObject"] = i["complaintObject"]
            item["description"] = i["description"]
            item["responseObject"] = i["responseObject"]
            item["response"] = i["response"]

            yield item