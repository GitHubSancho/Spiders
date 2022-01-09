# 解析页面
class ParsingWeb:
    js_parse = [
        "http://jx.jx66.cf/?url=",
        "http://jx.mmkv.cn/tv.php?url=",
        "http://www.asys.vip/jx/?url=",
        "http://www.freeget.org/jx.php?url=",
        "https://123.xxgcx.cn:4433/jx.php?url=",
        "https://api.jiexi.la/?url=",
        "https://jiexi.8old.cn/m3u8tv20210705%60/?url=",
        "https://jx.aidouer.net/?url=",
        "https://jx.m3u8.tv/jiexi/?url=",
        "https://jx.parwix.com:4433/player/?url=",
        "https://jx.parwix.com:4433/player/analysis.php?v=",
        "https://jx.xmflv.com/?url=",
        "https://jx.ysgc.xyz/?url=",
        "https://m2090.com/?url=",
        "https://okjx.cc/?url=",
        "https://sb.5gseo.net/?url=",
        "https://svip.jiangxs.vip/player/?url=",
        "https://vip.bljiex.com/?v=",
        "https://vip.parwix.com:4433/player/?url=",
        "https://www.2ajx.com/vip.php?url=",
        "https://www.feiyuege.cf/?url=",
        "https://www.feiyuege.cf/v/jiexi/?url=",
        "https://www.feiyuege.cf/v/jiexi2/?url=",
        "https://www.pangujiexi.com/jiexi/?url=",
        "https://wyjx.qd234.cn/player/?url=",
    ]

    # 完整的搜索页面链接 = 搜索链接 + 影名
    def search_port(self, mv_name):
        urls = []
        # urls.append(
        #     f"https://www.4ltv.com/search/-------------.html?wd={mv_name}")
        urls.append(f"https://so.iqiyi.com/so/q_{mv_name}")
        urls.append(f"https://search.bilibili.com/pgc?keyword={mv_name}")
        urls.append(f"https://v.qq.com/x/search/?q={mv_name}")
        urls.append(f"https://so.youku.com/search_video/q_{mv_name}")
        return urls

    # 接受页面的url，判断是哪个网站，跳转到该网站解析方法，传递response
    def read_page(self, url, rsp):
        site = url.split("/")[2]
        if site == "www.4ltv.com":
            return self.port_4ltv(url, rsp)
        elif site == "so.iqiyi.com":
            return self.port_iqiyi(url, rsp)
        elif site == "search.bilibili.com":
            return self.port_bilibili(url, rsp)
        elif site == "v.qq.com":
            return self.port_qq(url, rsp)
        elif site == "so.youku.com":
            return self.port_youku(url, rsp)
        else:
            print("未找到")
            #关闭爬虫
            self.close()

    # 返回搜索页面的xpath
    # 返回info结构为：
    # [是否需要二次访问，返回电影页面的url,返回url头部（用于拼接出完整url）,js解析接口列表]
    def port_4ltv(self, url, rsp):
        return [False]

    def port_iqiyi(self, url, rsp):
        return [
            True, '//*[@class="qy-search-result-btn"]/@href', "https:",
            self.js_parse
        ]

    def port_bilibili(self, url, rsp):
        return [False]

    def port_qq(self, url, rsp):
        return [False]

    def port_youku(self, url, rsp):
        return [False]
