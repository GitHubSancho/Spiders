from selenium.webdriver import Chrome
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time

#1.创建浏览器对象
web = Chrome()

#2.打开一个网址
web.get("https://www.zhipin.com/c101270100-p101399/?ka=search_101399")

#3.解析页面信息，进行数据提取
uls = web.find_elements(By.XPATH, '//*[@id="main"]/div/div[3]/ul/li')
for ul in uls:
    jobs = ul.find_element(By.XPATH,
                           "./div/div[1]/div[1]/div/div[1]/span[1]/a").text
    coms = ul.find_element(By.XPATH, './div/div[1]/div[2]/div/h3/a').text
    area = ul.find_element(By.XPATH,
                           './div/div[1]/div[1]/div/div[1]/span[2]/span').text
    price = ul.find_element(By.XPATH,
                            "./div/div[1]/div[1]/div/div[2]/span").text
    skills = [
        i.text for i in ul.find_elements(By.XPATH, './div/div[2]/div[1]/span')
        if i.text != ""
    ]
    print(price, jobs, coms, area, skills)