# *-* coding:utf8 *-*

# 需要安装,发送请求用
import urllib.parse

import requests
# 需要安装,解析数据用
from lxml import etree
# 正则
import re

from spider_server.conf.config_util import ConfigUtil
from spider_server.db.db_ocop import OcopDB
from spider_server.logs.logger import Logger

"""
1 准备URL列表
2 遍历URL,发送请求,获取响应数据
3 解析数据
4 保存数据
"""
logger = Logger(__name__).get_log()


class BeiJing2017wl(object):
    def __init__(self):
        """初始化数据"""
        # 准备url模板
        self.url = ConfigUtil().get("OCOP", "BEIJING_2017_WL")
        # 省份名称
        self.sydsfmc = "北京"
        # 录取年份
        self.lqnf = 2017
        # 指定请求头
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
        }

    def run(self):
        """程序入口,核心入口"""
        logger.info("-------------------北京-2017-文/理--------------------")
        # 判断数据是否存在
        if not self.is_exist():
            # 1 发送请求,获取响应数据
            page = self.get_page_from_url(self.url)
            # 2 解析数据
            datas = self.get_datas_from_page(page)
            # 3 保存数据
            self.save_data(datas)
        logger.info("----------------------------------------------------")

    def get_page_from_url(self, url):
        """根据url,发送请求,获取页面数据"""
        response = requests.get(url, headers=self.headers)
        # 返回响应的字符串数据,二进制需要转为字符串
        return response.content

    def get_datas_from_page(self, page):
        """解析页面数据"""
        # 页面转换为Element,就可以使用Xpath提取数据了
        element = etree.HTML(page)
        # 获取标签列表
        # xpath返回的是一个列表
        trs = element.xpath("//*/table/tbody/tr")
        # 遍历列表,提取需要的数据
        data_list = []
        # 文史类数据
        i = 0
        for tr in trs:
            i += 1
            # 过滤头尾数据
            if i <= 4 or i == len(trs):
                logger.info("".join(tr.xpath("./td//text()")).strip())
                continue
            item = {}
            # ./ 表示当前路径之下,// 表示获取该节点及其之下所有的文本
            item["lqnf"] = self.lqnf
            item["sydsfmc"] = self.sydsfmc
            item["klmc"] = "文史类(含全国性加分)"
            # 分数段
            range_score = "".join(tr.xpath("./td[1]//text()")).replace("分以上", "").split("→")
            if len(range_score) == 2:
                # 分数上限
                item["fssx"] = int(range_score[1].strip())
                # 分数下限
                item["fsxx"] = int(range_score[0].strip())
            else:
                item["fssx"] = int(range_score[0].strip())
                item["fsxx"] = int(range_score[0].strip())
            # 本分数段人数
            item["bfsdrs"] = int(
                "0" if tr.xpath("./td[2]//text()")[0].strip() == "" else tr.xpath("./td[2]//text()")[0].strip())
            # 本分数段累计人数
            item["bfsdljrs"] = int(
                "0" if tr.xpath("./td[3]//text()")[0].strip() == "" else tr.xpath("./td[3]//text()")[0].strip())
            logger.info("记录：%s---->%s" % (i, item))
            data_list.append(item)
        # 理工类数据
        i = 0
        for tr in trs:
            i += 1
            # 过滤头尾数据
            if i <= 4 or i == len(trs):
                logger.info("".join(tr.xpath("./td//text()")).strip())
                continue
            item = {}
            # ./ 表示当前路径之下,// 表示获取该节点及其之下所有的文本
            item["lqnf"] = self.lqnf
            item["sydsfmc"] = self.sydsfmc
            item["klmc"] = "理工类(含全国性加分)"
            # 分数段
            range_score = "".join(tr.xpath("./td[1]//text()")).replace("分以上", "").split("→")
            if len(range_score) == 2:
                # 分数上限
                item["fssx"] = int(range_score[1].strip())
                # 分数下限
                item["fsxx"] = int(range_score[0].strip())
            else:
                item["fssx"] = int(range_score[0].strip())
                item["fsxx"] = int(range_score[0].strip())
            # 本分数段人数
            item["bfsdrs"] = int(
                "0" if tr.xpath("./td[4]//text()")[0].strip() == "" else tr.xpath("./td[4]//text()")[0].strip())
            # 本分数段累计人数
            item["bfsdljrs"] = int(
                "0" if tr.xpath("./td[5]//text()")[0].strip() == "" else tr.xpath("./td[5]//text()")[0].strip())
            logger.info("记录：%s---->%s" % (i, item))
            data_list.append(item)
        return data_list

    def save_data(self, datas):
        """保存数据"""
        # 保存基础数据
        OcopDB().save_ocop_base_data(
            {"lqnf": self.lqnf, "sydsfmc": self.sydsfmc, "klmc": "文史类(含全国性加分)", "wldz": self.url})
        OcopDB().save_ocop_base_data(
            {"lqnf": self.lqnf, "sydsfmc": self.sydsfmc, "klmc": "理工类(含全国性加分)", "wldz": self.url})
        # 保存明细数据
        OcopDB().save_ocop_detail_data(datas)

    def is_exist(self):
        """判断数据知否存在"""
        # 判断基础数据是否存在
        base_count = OcopDB().get_ocop_base_count(self.sydsfmc, self.lqnf, "")
        detail_count = OcopDB().get_ocop_detail_count(self.sydsfmc, self.lqnf, "")
        logger.info("base_count---->%s" % base_count)
        logger.info("detail_count---->%s" % detail_count)
        if base_count == 0 and detail_count == 0:
            return False
        return True


if __name__ == '__main__':
    BeiJing2017wl().run()
