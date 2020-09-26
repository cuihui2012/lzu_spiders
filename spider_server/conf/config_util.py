# -*- coding: utf-8 -*-

import configparser
import os

from spider_server.logs.logger import Logger

logger = Logger(__name__).get_log()


class ConfigUtil(object):
    # 类属性：记录实例是否存在
    instance = None

    # 初始化执行标志
    init_flag = False

    # 创建对象,首先会为对象分配空间
    def __new__(cls, *args, **kwargs):
        # 判断类属性是否为空对象
        if cls.instance is None:
            # 调用父类方法,为对象分配空间
            logger.info("ConfigUtil-->分配空间")
            cls.instance = super().__new__(cls)
        # 返回对象的引用
        return cls.instance

    # 分配空间后,对象属性初始化
    def __init__(self):
        # 判断是否执行过初始化动作
        if ConfigUtil.init_flag:
            return
        logger.info("ConfigUtil-->对象属性初始化")
        cfgparser = configparser.ConfigParser()  # 系统配置

        # 以当前文件为准找config.ini文件
        cfgpath = os.path.join(os.path.abspath(os.path.join(__file__, "../../..")), "config.ini")
        logger.info("config.ini------>%s" % cfgpath)
        # 读ini文件
        cfgparser.read(cfgpath, encoding="utf-8")  # python3

        self.conf = cfgparser
        ConfigUtil.init_flag = True

    def __del__(self):
        # logger.info("del done!")
        pass

    # 获取配置值
    def get(self, section, option):
        # 返回的都是字符串
        return self.conf.get(section, option)


if __name__ == '__main__':
    str = "http://www.mafengwo.cn/search/q.php?q={}&p={}&t=pois&kt=1".format("西安", None)
    logger.info(str)
