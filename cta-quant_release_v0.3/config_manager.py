import json


class ConfigManager(object):
    '''
    配置中心，加载每个账户的配置文件
    '''

    def __init__(self, config_path) -> None:
        self.config_path = config_path
        self.config_data = self._load_config()

    def _load_config(self):
        try:
            with open(self.config_path, "r", encoding="utf-8") as file:
                return json.load(file)
        except FileNotFoundError:
            raise Exception(f"配置文件未找到: {self.config_path}")
        except json.JSONDecodeError:
            raise Exception(f"配置文件格式错误（不是有效的JSON）: {self.config_path}")
        except Exception as e:
            raise Exception(f"读取配置文件时发生错误: {e}")

    def get(self, key, default=None):
        return self.config_data.get(key, default)

    def set(self, key, value):
        self.config_data[key] = value

    def reload_config(self):
        self.config_data = self._load_config()
