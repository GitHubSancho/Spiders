#!/usr/bin/env python
#-*- coding: utf-8 -*-
#FILE: main.py
#CREATE_TIME: 2022-08-23
#AUTHOR: Sancho

class Engine:
    def __init__(self) -> None:
        # 读取配置文件
        self.loader = Loader()
        self.conf, self.hubs = self.loader.reload_files()
        ua_list = self.loader.load_ua()
        # 连接数据库
        self.mongo = Mongo(self.conf)
        # 其它模块
        self.downloader = Downloader(ua_list)
        self.parser = Parser()
        # self.proxy = Proxy()
        # 初始化变量
        self.failure_pool = {}

    def __del__(self):
        self._close()

    @staticmethod
    def get_hosts(urls):
        if isinstance(urls, str):
            return urlparse.urlparse(urls).netloc
        return [urlparse.urlparse(url).netloc for url in urls]

    @staticmethod
    def pack_documents(mode, urls):
        hosts = Engine.get_hosts(urls)
        documents = Mongo._build_defult_documents(mode, zip(urls, hosts))
        return hosts, documents

    def _close(self):
        sys.exit("done!")

    async def refresh_files(self, refreshtime=300):
        if time.time() - self.last_loading_time < refreshtime:
            return
        self.last_loading_time = time.time()
        conf_new, hubs_new = self.loader.reload_files()
        if not conf_new and not hubs_new:
            return
        if self.conf != conf_new:
            self.conf = conf_new
        if self.conf['exit'] is True:  #退出检测
            self._close()
        if difference := set(hubs_new).difference(set(self.hubs)):  # 添加到数据库
            hubs_hosts, documents = self.pack_documents('hub', difference)
            self.hubs_hosts.extend(hubs_hosts)
            await self.mongo.update_many(documents)
            self.hubs = hubs_new

        return

    async def tab_url(self, features):
        htmls = []
        urls = {}
        hubs = []
        for status_code, html, url, _0, _1 in features:
            if status_code != 200:  # 标记访问错误
                # self._proxy = self.proxy.sub_proxy_count(_1)
                if self.failure_pool.get(url, False):
                    if self.failure_pool[url] >= 3:
                        del self.failure_pool[url]  # 访问失败三次从内存删除
                        continue
                    self.failure_pool[url] += 1  # 增加访问失败计数
                    continue
                self.failure_pool[url] = 1  # 添加访问失败计数
            if url in self.hubs:  # 拼接hub链接的html
                htmls.append((url, html))
                hubs.append(url)
                continue
            urls[url] = html  # 标记普通url html
        return htmls, hubs, urls

    def extract(self, htmls):
        links = process_pool.map(extract_links, htmls)
        if not links:
            return
        l = []
        [l.extend(link) for link in links if link]
        return self.parser._filter_good(l, self.hubs_hosts)

    def build_documents(self, hubs, urls, links):
        documents = []
        # 生成访问错误的文档
        if self.failure_pool:
            failure_documents = Mongo._build_failure_documents(
                self.failure_pool)
            documents.extend(failure_documents)
        # 生成hubs更新文档
        if hubs:
            hubs_documents = Mongo._build_update_documents('hub', hubs)
            documents.extend(hubs_documents)
        # 生成普通url文档
        if urls:
            _urls = urls.keys()
            _htmls = urls.values()
            _htmls_zip = process_pool.map(zip_html, _htmls)
            url_documents = Mongo._build_update_documents(
                'url', _urls, 'success', 0, _htmls_zip)
            documents.extend(url_documents)
        # 生成新url文档
        if links:
            links = [link for link in links if link not in self.urls]
            links_hosts = self.get_hosts(links)
            links_documents = Mongo._build_defult_documents(
                'url', zip(links, links_hosts))
            documents.extend(links_documents)
        return documents

    async def get_proxy(self):
        proxy = self.proxy.get_proxy_ip()
        result = await self.downloader.test_proxy(self.session, proxy)
        proxy = [t[4] for t in result if t if t[0] == 200]
        return self.proxy.build_proxy_pool(proxy)

    async def start(self):
        # 异步函数
        # 上传初始链接
        self.hubs_hosts, documents = self.pack_documents('hub', self.hubs)
        await self.mongo.update_many(documents)
        # 初始化变量
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.last_loading_time = time.time()
        self._proxy = None
        # assert await self.get_proxy()
        # self._proxy = self.proxy.choices(1)[0]
        print("- 开始抓取 -")
        while 1:
            # for _i in range(5):
            # 计时重读配置文件
            await self.refresh_files(60 * 2)
            # 获取待下载链接
            self.urls = await self.mongo.get_all_list()
            task = await self.mongo.get_waiting_urls(
                self.conf['pending_threshold'], self.conf['failure_threshold'],
                MAX_WORKERS_CONCURRENT)
            # 执行任务
            if not task:
                continue
            # if not self._proxy:
            #     await self.get_proxy()
            #     self._proxy = self.proxy.choices(1)[0]
            # 下载网页
            features = await self.downloader.crawler(self.session, task,
                                                     self._proxy)
            # 标记链接
            htmls, hubs, urls = await self.tab_url(features)
            # 解析网页
            # NOTE: 20   16863600.0 843180.0     31.9
            good_links = self.extract(htmls)
            # 生成文档
            # NOTE: 20   35603163.0 1780158.1     67.3
            documents = self.build_documents(hubs, urls, good_links)
            # 上传文档
            await self.mongo.update_many(documents)
            # 查看状态
            print(
                f'已下载页面/全部链接： {await self.mongo.len_downloaded_documents()}/{len(await self.mongo.get_all_list())}'
            )

    def run(self):
        try:
            # 协程模块
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(self.start())
        except KeyboardInterrupt:
            print('stopped by yourself!')
        self._close()


def extract_links(document):
    parser = Parser()
    return parser._extract_links_re(document[0], document[1])


def zip_html(html):
    if not html:
        return ''
    if isinstance(html, str):
        html = html.encode('utf8')
    return lzma.compress(html)


def check_memory():
    # 检查内存
    mem = psutil.virtual_memory()
    zj = float(mem.total) / 1000000000
    ysy = float(mem.used) / 1000000000
    return int((0.8 * zj - ysy) / 0.16 * 24)


if __name__ == "__main__":
    MAX_WORKERS_CONCURRENT = check_memory()
    # TEST_URL = 'https://www.baidu.com'
    # PROXYPOOL_URL = 'http://webapi.http.zhimacangku.com/getip'
    # PROXYPOOL_API_KEY = '262150'
    # PROXYPOOL_API_PARAMS = {
    #     'num': 100,
    #     'type': 3,
    #     'pro': 0,
    #     'city': 0,
    #     'yys': 0,
    #     'port': 1,
    #     'time': 1,
    #     # 'pack': PROXYPOOL_API_KEY,
    #     'ts': 0,
    #     'ys': 0,
    #     'cs': 0,
    #     'lb': 2,
    #     'sb': 0,
    #     'pb': 45,
    #     'mr': 1,
    #     'regions': ''
    # }
    with ProcessPoolExecutor() as process_pool:
        engine = Engine()
        engine.run()
