scrapy安装
mac or linux : pip install scrapy
windows:
	pip install wheel
	download for https://www.lfd.uci.edu/~gohlke/pythonlibs/#twisted
	cd twisted所在文件夹
	pip install ./twisted文件名
	pip install pywin32
	pip install scrapy
	测试scrapy命令

scrapy基本使用
	scrapy创建工程：scrapy startproject fileName
	进入工程文件夹：cd fileName
	创建工程文件：scrapy genspider spiderName  起始url
	执行工程： scrapy crawl spiderName
	执行工程返回状态（不返回日志信息）： scrapy crawl spiderName --nolog
	关闭robottstxt:打开settings文件，找到ROBOTSTXT_OBEY = True，True改为False
	只显示指定类型的日志信息：打开settings文件，添加LOG_LEVEL项，如：LOG_LEVEL = 'ERROR'
	修改UA标识：打开settings文件，添加USER_AGENT项，如：USER_AGENT =  'Mozilla/5.0...'

scrapy持久化存储
	基于终端指令：
		scrapy crawl spiderName -o dir/fileName.csv
		只可以将parse方法的返回值存储到本地的文件中
		文件类型只可以为json,jsonlines,lj,csv,xml,marshal,pickle
	基于管道：
		编码流程：数据解析 → 在item类中定义相关属性 → 封装存储到item类型对象 
			→ 将item类型对象提交给管道 → 在管道类的process_item中将接收到的item对象中存储数据
			→ 在配置文件中开启管道（ITEM_PIPELINES项，数值越小优先级越高）