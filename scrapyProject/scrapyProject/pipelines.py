# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class ScrapyprojectPipeline:
    fp = None

    # 重写父类初始化
    def open_spider(self, spider):
        print("开始爬取")
        # 定义文件
        self.fp = open("./Complaint.csv", "w", encoding="utf_8_sig")

    def process_item(self, item, spider):
        # 定义属性
        name = item["name"]
        title = item["title"]
        complaintObject = item["complaintObject"]
        description = item["description"]
        responseObject = item["responseObject"]
        response = item["response"]
        # 写入文件
        # self.fp.write(
        #     '{mame},{title},{complaintObject},{description},{responseObject},{response}\n'
        #     .format(name, title, complaintObject, description, responseObject,
        #             response))
        self.fp.write('{},{},{},{},{},{}\n'.format(name, title,
                                                   complaintObject,
                                                   description, responseObject,
                                                   response))

        return item

    # 结束程序
    def close_spider(self, spider):
        print("结束爬取")
        self.fp.close()
