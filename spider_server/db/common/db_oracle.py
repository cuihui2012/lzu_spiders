# *-* coding:utf8 *-*

import cx_Oracle

from spider_server.conf.config_util import ConfigUtil


class DBOracle(object):
    def __init__(self, host=ConfigUtil().get("DB", "DB_HOST"), port=int(ConfigUtil().get("DB", "DB_PORT")),
                 db=ConfigUtil().get("DB", "DB"), user=ConfigUtil().get("DB", "USER"),
                 passwd=ConfigUtil().get("DB", "PASSWD")):
        # 建立连接
        self.conn = cx_Oracle.connect("%s/%s@%s:%s/%s" % (user, passwd, host, port, db))
        # 创建游标，操作设置为字典类型
        self.cur = self.conn.cursor()

    def __enter__(self):
        """with语法,返回的值赋值给as后变量"""
        # 返回游标
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        """with语法,内容执行完毕后,会执行该方法"""
        # 提交数据库并执行
        self.conn.commit()
        # 关闭游标
        self.cur.close()
        # 关闭数据库连接
        self.conn.close()
