### scrapy安装
#### mac or linux : 
	pip install scrapy
#### windows:
	pip install wheel
	download for https://www.lfd.uci.edu/~gohlke/pythonlibs/#twisted
	cd twisted所在文件夹
	pip install ./twisted文件名
	pip install pywin32
	pip install scrapy
	测试scrapy命令

### scrapy基本使用
	scrapy创建工程：scrapy startproject fileName
	进入工程文件夹：cd fileName
	创建工程文件：scrapy genspider spiderName  起始url
	执行工程： scrapy crawl spiderName
	执行工程返回状态（不返回日志信息）： scrapy crawl spiderName --nolog
	关闭robottstxt:打开settings文件，找到ROBOTSTXT_OBEY = True，True改为False
	只显示指定类型的日志信息：打开settings文件，添加LOG_LEVEL项，如：LOG_LEVEL = 'ERROR'
	修改UA标识：打开settings文件，添加USER_AGENT项，如：USER_AGENT =  'Mozilla/5.0...'

### scrapy持久化存储
#### 基于终端指令：
	scrapy crawl spiderName -o dir/fileName.csv
	#只可以将parse方法的返回值存储到本地的文件中
	#文件类型只可以为json,jsonlines,lj,csv,xml,marshal,pickle
#### 基于管道：
	数据解析 → 在item类中定义相关属性 → 封装存储到item类型对象 
	→ 将item类型对象提交给管道 → 在管道类的process_item中将接收到的item对象中存储数据
	→ 在配置文件中开启管道（ITEM_PIPELINES项，数值越小优先级越高）

### scrapy五大核心组件
	引擎（scrapy）：处理整个系统的数据流、触发事件
	调度器（scheduler）：接收引擎发送的请求链接，去重和压入队列，在引擎再次请求时返回
	下载器（downloader）：下载网页内容，并返回网页内容给spiders（采用twisted异步模块）
	爬虫（spiders）：用于解析信息封装到item
	项目管道（pipeline）：处理返回的item，验证item有效性、清除不需要的信息，进行持久化存储

### scrapy请求传参
	使用场景：爬取和解析的数据在多个页面（深度爬取）
	parse手动请求：
		添加item将封装的信息
		导入item模块
		定义:item = 将封装信息的Item()
		传入：yield scrapy.Request(链接,callback=self.子解析函数,meta={'item'}:item)
		回调到子解析函数，接收item提交：yield item
		开启管道来接收item
	分页操作：
		parse的for循环解析后，增加页面迭代判断
		然后yield scrapy.Request(新分页链接,callback=self.parse)

### scrapy图片爬取(ImagesPipeline)
	区别：Xpath解析到图片src属性值后，不需要获取src内的数据，封装到item后ImagesPipeline会自动获取图片二进制类型的数据
	流程：
		导入items类：from 工程名.items import Item类名
		将parse解析到的src压入item类：
			for ... :
				item = Item类名()
				....
				yield item
		在item类中封装属性：src = scrapy.Field()
		导入管道类中的图片模块:from scrapy.pipelines.images import ImagesPipeline
		封装图片管道类:class 自定义类名(ImagesPipeline):
			重写管道方法：
				def get_media_requests(self,item,info):#根据图片地址请求图片数据的类
					yield scrapy.Request(item['src'])
				def file_path(self,request,response=None,info=None):#指定图片存储路径
					imgName = request.url.split('/')[-1]#传递来的图片url的末尾字符作为图片名
					return imgName
				def item_completed(self,results,item,info):
					return item #返回给下一个即将被执行的管道类
		指定图片存储目录：
			settings.py中添加：IMAGES_STORE='./imgs'#表示图片的存储目录
			在settings.py中指定：ITEM_PIPELINES={'当前管道类类名':'300',}

### scrapy下载中间件
	下载中间件：在引擎和下载器之间，拦截整个工程中的请求和响应
	作用：拦截请求：局部UA伪装，代理IP；拦截响应：篡改响应数据、响应对象
		pricess_request方法：拦截请求
			#局部UA伪装：
			request.headers["User-Agent"] = random.choice(self.UA池)
			return request
		process_response方法：拦截响应
			篡改响应：
				#在爬虫文件中实例化selenium浏览器对象用于爬取动态加载数据
					def __init__(self): # 在爬虫类中定义
						from selenium import webdriver
						self.bro = webdriver.Chrome(executable_path='浏览器驱动地址')
				#在process_response方法中判断挑选请求对象：
					#获取爬虫类中定义的浏览器对象
						bro = spider.bro
					if request.url in sipider.models_urls:#判断当前url是否在子链接列表中
						bro.get(request.url) # 拿到的子链接url进行请求
						sleep(3)
						page_text = bro.page_source # 包含了动态加载的数据
						from scrapy.http import HtmlResponse
						# 实例化新响应对象（包含动态加载出的数据），替代原来旧的响应对象			
						new_response = HtmlResponse(url=request.url,body=page_text,encoding='utf-8',request=request)
						return 
					else: 
						return response # 其它链接响应对象
				#在爬虫文件结束后关闭浏览器对象
					def closed(self,spider):
						self.bro.quit()
		process_exception方法：拦截发生异常的请求
			异常请求使用代理IP重新请求：
			request.meta['proxy'] = random.choice(self.IP池) # 注意判断http和https协议头
			return request # 重新发送修改后的请求
		开启中间件：在settings.py中启用DOWNLOADER_MIDDLEWARES项

### CrawlSpider类
	Spider的一个子类
	基于全站数据爬取的方式
	CrawlSpider使用
		创建工程
		切换到工程目录：cd spiderProject
		创建CrawSpider爬虫文件：scrapy genspider -t crawl spiderName www.xxx.com 
		新导入的类解释：
			LinkExtractor：链接提取器，根据指定规则(allow=r'正则')进行指定链接的提取
			Rule:规则解析器,根据LinkExtractor提取到的链接进行指定规则(callback)的解析操作，follw=是否循环将提取器作用到子页面
