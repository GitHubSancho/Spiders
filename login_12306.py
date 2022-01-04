from selenium.webdriver import Chrome
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
# from chaojiying import Chaojiying_Client  #需要注册账户且账户内有余额,并下载api文件
import time
"""
模拟登陆12306

1.验证码识别api初始化（删除）
2.隐藏chromeDriver信息躲避拦截
3.打开12306登陆界面
4.获取图片验证码传入验证码识别api（删除）
5.根据验证码识别api返回坐标找到验证码对应坐标并点击（删除）
6.录入帐号密码
7.移动滑块识别

# 由于新版页面没有图片验证码，所以不必使用验证码识别api
"""

#1.初始化验证码识别api
# chaojiying = Chaojiying_Client("帐号", "密码", "软件ID")  #软件ID查看网站个人中心

#2.隐藏chromeDriver信息
# chrome版本 >= 88
option = Options()
option.add_experimental_option("excludeSwitches", ["enable-automation"])
option.add_argument("--disable-blink-features=AutomationControlled")
web = Chrome(options=option)

# chrome版本 < 88
# web = Chrome()
# web.execute_cdp_cmd(
#     "Page.addScriptToEvaluateOnNewDocument", {
#         "source":
#         """
#                     navigator.webdriver = undefined
#                     Object.defineProperty(navigator, 'webdriver', {
#                       get: () => undefined
#                     })
#                   """
#     })
# web.get("http:xxx")

#3.打开12306登录界面
web.get("https://kyfw.12306.cn/otn/resources/login.html")
time.sleep(3)

#4.识别图片验证码
# 定位
# web.find_element(By.XPATH,"帐号登录xpath").click() # 新版已默认帐号密码登录，无需点击
# time.sleep(3)
# 获取图片验证码
# verify_img_element = web.find_element(By.XPATH,"验证码图片XPATH")
# 识别图片验证码
# dic = chaojiying.PostPic(verify_img_element.screenshot_as_png,
#                          9004)  # 9004代表返回最多4组坐标型数据，详情见网站收费类型
# result = dic["pic_str"]  #返回x1,y1|x2,y2|....

#5.点击验证码坐标
# 分割坐标
# rs_list = result.split("|")
# for rs in rs_list:  # x1,y1
#     p_temp = rs.split(",")
#     x = int(p_temp[0])
#     y = int(p_temp[1])
#     # 创建事件链，注意动作需要perform函数才能执行
#     ActionChains(web).move_to_element_with_offset(verify_img_element,x,y).click().perform()
# time.sleep(3)

#6.录入账户和密码
# 定位输入框位置,并输入信息
web.find_element(By.XPATH, '//*[@id="J-userName"]').send_keys("帐号123456")
web.find_element(By.XPATH, '//*[@id="J-password"]').send_keys("密码123456")
# 点击登录
web.find_element(By.XPATH, '//*[@id="J-login"]').click()
time.sleep(5)

#7.移动滑块验证
# 定位滑块
btn = web.find_element(By.XPATH, '//*[@id="nc_1_n1z"]')
ActionChains(web).drag_and_drop_by_offset(btn, 300,
                                          0).perform()  # 拖拽命令，300，0是相对坐标移动x,y轴
