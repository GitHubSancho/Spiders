import sys
import yaml

class Loader:
    def __init__(self) -> None:
        my_dir = sys.path[0]
        my_name = my_dir.split('\\')[-1]
        self.path = f'{sys.path[0]}\\{my_name}'

    def load_conf(self):
        with open(f'{self.path}.yml', 'r', encoding='utf-8') as f:
            conf = yaml.load(f, Loader=yaml.CLoader)
        return conf

    def load_hubs(self):
        with open(f'{self.path}_hubs.yml', 'r', encoding='utf-8') as f:
            hubs = yaml.load(f, Loader=yaml.CLoader)
            hubs = list(set(hubs))
        return hubs

    def reload_files(self):
        conf = self.load_conf()  # 读取配置文件
        hubs = self.load_hubs()  # 读取链接列表
        return conf, hubs

    def load_ua(self):
        with open(f'{self.path}_ua.yml', 'r', encoding='utf-8') as f:
            ua_list = yaml.load(f, Loader=yaml.CLoader)
        return ua_list