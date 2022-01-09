import scrapy
import pandas as pd
from movieDownload.model.parsingWeb import ParsingWeb


class GeturlSpider(scrapy.Spider):
    name = 'getUrl'
    start_urls = []
    page = []
    mv_name = input("请输入电影名:")
    # 搜索接口
    parsingweb = ParsingWeb()
    start_urls = parsingweb.search_port(mv_name)

    def parse(self, response):
        if response.status == 200:
            site = self.parsingweb.read_page(response.url, response.text)
            if site[0] == True:
                #解析搜索页面，得到链接，合并成完整页面链接
                url_list = response.xpath(site[1]).extract()
                pageUrl_list = [site[2] + i for i in url_list]
                #合并JS解析接口地址+源视频地址，等待访问解析接口
                js_parse = [js + url for js in site[3] for url in pageUrl_list]
                print(js_parse)
                # yield scrapy.Request(site[1], callback=self.parse_js)
            elif site[0] == False:
                #TODO: 如果不需要二次访问，解析网页架构，找到子链接，根据子链接找到播放页面，根据播放页面的video标签下载m3u8文件
                pass

    def parse_js(self, response):
        #访问js解析网站解析后的response
        pass

    def close(self, spider):
        pass


"""
豆瓣top250片源数量
顶空影视        505
思乐影视        462
爱奇艺         186
牛马TV        172
瓜皮TV        156
逗趣影视        154
GAZE        138
LIBVIO      137
bilibili    136
批哩啪哩         96
低端影视         96
片库           91
腾讯视频         53
动漫星球         46
绘盒           32
优酷           15
31看影视        11
奈菲影视          3
"""
