|项目名称|介绍|优化|改进方向|
|----|----|----|----|
| get_163music_comment_.py |<li>网易云评论获取，不能自动化运行，仅作为练习项目<br><li>代码量并不多，但涉及JS二次加密，需要逆向模拟加密过程<br>换其它音乐下的评论获取，需要得到网页加密参数 |
| get_jobInfo_boss.py | <li>实现程序化打开网页，并模拟人工操作<br><li>需要依赖selenium库，和下载使用chromeDriver<br><li>优缺点很明显，需要等待网页完全接包才能操作，程序操作之间需要等待，获取数据很慢<br>但可以绕过一些JS繁琐的加密程序，编写起来也简单易懂 |（2022/01/04)<br><li>增加了自动读取多页|<li>增加自定义职位搜索、城市选择|
|get_taobao.py|未完成作品 淘宝搜索器|||
|login_12306.py |<li>简单的selenum练习，需要反反爬，创建事件链等操作|
|m3u8_downloader.py|<li>m3u8解析器，涉及多json解析，js加密，异步爬虫，大批量文件合并等技术，还挺麻烦<br><li>运行后可以下载电影，修改url地址可以下载其他电影（仅链接中网站)<br><li>其他网站需要修改地址，匹配方式，解析条件，解密方式等，都大差不离| |<li>优化读写<br><li>加入进度条<br><li>加入多线程设计<br><li>异步删除不需要的ts文件<br><li>设置ip池和下载超时增加断点续传功能<br><li>优化合并速度|
|pic_umei2.py|<li>异步爬虫的一次小尝试<br><li>运行后自动下载优美图片网唯美图片分页页面中的一页大图图片；<br><li>下载更多图片可以循环调用get_urls，需要在网址后加上"index_2.htm",数字2为页面标记以此类推；<br><li>其它分页也差不多；| |<li>需要和多线程（多进程）比较速度，或多线程+异步看看能提速多少<br><li>使用了global关键字调用全局变量，而不是调取返回值的方式获取下载链接|
|proxypool_ip3366.py|<li>一个爬取代理IP的爬虫，方便之后爬虫使用。<br><li>运行即可自动获取和验证，输出可用IP，同目录下生成"proxypool.csv"文件根据延迟升序|(2021/12/29)<br><li>重构，增加代码复用性，以每个页面为单位<br><li>优化了验证速度，最终速度取决于最耗时的页面内IP|<li>调用更多接口和增加自定义接口|
|translator_baidu.py|<li>输入单词自动获取百度翻译结果<br><li>（无聊的JS加密练习<br>运行后需要输入查询的单词回车即可得到翻译结果||<li>增加其它翻译接口供选择<br><li>支持批量输入或文件读取<br><li>增加文本纠错|