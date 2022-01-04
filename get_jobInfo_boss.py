from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
import time
import pandas as pd

#1.创建浏览器对象
web = Chrome()

#2.打开一个网址
web.get("https://www.zhipin.com/c101270100-p101399/?ka=search_101399")
# 等待打开
time.sleep(3)
# 定位下一页
nexts = web.find_element(By.XPATH,
                         '//*[@id="main"]/div/div[3]/div[3]/a[last()]')
infos = pd.DataFrame()  # 记录信息
# 进入信息获取循环
while True:
    #3.定位信息栏列表
    uls = web.find_elements(
        By.XPATH, '//*[@class="job-list"]/ul/li')  #第二页开始页面div有变动，用class定位
    #4.从信息栏列表节点中提取每列
    for ul in uls:
        # 从每个节点中提取信息，加入pandas
        jobs = ul.find_element(By.XPATH,
                               "./div/div[1]/div[1]/div/div[1]/span[1]/a").text
        coms = ul.find_element(By.XPATH, './div/div[1]/div[2]/div/h3/a').text
        area = ul.find_element(
            By.XPATH, './div/div[1]/div[1]/div/div[1]/span[2]/span').text
        price = ul.find_element(By.XPATH,
                                "./div/div[1]/div[1]/div/div[2]/span").text
        skills = [
            i.text
            for i in ul.find_elements(By.XPATH, './div/div[2]/div[1]/span')
            if i.text != ""
        ]
        infos = infos.append(pd.DataFrame(
            [[price, jobs, coms, area, skills]],
            columns=["薪资", "职位", "公司", "地点", "关键词"]),
                             ignore_index=True)
    # 查看本页页码
    cur = web.find_element(By.XPATH, '//*[@ka="page-cur"]').text
    print("已获取第" + cur + "页")

    #5.点击下一页
    nexts.click()
    # 等待点击刷新
    time.sleep(3)
    nexts = web.find_element(By.XPATH,
                             '//*[@id="main"]/div/div[2]/div[2]/a[last()]'
                             )  #第四页Xpath位置会有变动，使用last函数获取最后一个a标签

    #6.判断有没有下一页，没有则退出
    if nexts.get_attribute("href") == "javascript:;":
        break

# 设置输出样式
pd.set_option('display.unicode.east_asian_width', True)  #右对齐
# 输出获取的信息
print(infos)
# 将输出信息
infos.to_csv("jobs.csv", encoding="utf_8_sig", index=False)
