# Spiders/ 
自己制作的一些小爬虫会陆续收录到此仓库

## Spiders/get_163music_comment_.py
网易云评论获取，不能自动化运行，仅作为练习项目  
代码量并不多，但是挺复杂的，涉及JS二次加密，需要逆向模拟加密过程  
换其它音乐下的评论获取，需要得到网页加密参数


## Spiders/m3u8_downloader.py
muu8解析器，涉及多json解析，js加密，异步爬虫，大批量文件合并等技术，还挺麻烦

运行后可以下载电影，修改url地址可以下载其他电影（仅链接中网址）  
其他网站需要修改地址，匹配方式，解析条件，解密方式等，都大差不离

### 改进方向：
  * 优化读写
  * 加入进度条
  * 加入多线程设计
  * 异步删除不需要的ts文件
  * 设置ip池和下载超时
  * 增加断点续传功能
  * 优化合并速度


## Spiders/pic_umei2.py
异步爬虫的一次小尝试

运行后自动下载优美图片网唯美图片分页页面中的一页大图图片；  
下载更多图片可以循环调用get_urls，需要在网址后加上"index_2.htm",数字2为页面标记以此类推；  
其它分页也差不多；  

### 改进方向：
  * 需要和多线程（多进程）比较速度，或多线程+异步看看能提速多少
  * 使用了global关键字调用全局变量，而不是调取返回值的方式获取下载链接


## Spiders/proxypool_ip3366.py 
一个爬取代理IP的爬虫，方便之后爬虫使用。

运行即可自动获取和验证，输出可用IP，同目录下生成"proxypool.csv"文件根据延迟升序

### 改进方向：
  * 调用更多接口和增加自定义接口

### 优化问题(2021/12/29)：
  1. 重构，增加代码复用性，以每个页面为单位
  2. 优化了验证速度，最终速度取决于最耗时的页面内IP


## Spiders/translator_baidu.py
输入单词自动获取百度翻译结果。（无聊的JS加密练习）

运行后需要输入查询的单词回车即可得到翻译结果

### 改进方向：
  * 增加其它翻译接口供选择
  * 支持批量输入或文件读取
  * 增加文本纠错

