# *-* coding:utf8 *-*

from spider_server.db.common import db_oracle
from spider_server.logs.logger import Logger

logger = Logger(__name__).get_log()


class OcopDB(db_oracle.DBOracle):

    def save_ocop_base_data(self, data):
        """保存一分一段基本数据

        :param data:
        :return:
        """
        with OcopDB() as db:
            # SQL 插入语句
            sql = """
                        INSERT INTO LQ_YFYDWLDZ
                          (LQNF, SYDSFMC, KLMC, WLDZ, LRFSM)
                        VALUES
                          ({}, '{}', '{}', '{}', '1')
                        """.format(data["lqnf"], data["sydsfmc"], data["klmc"], data["wldz"])
            try:
                # 执行sql语句
                db.execute(sql)
            except Exception as result:
                # 发生错误时回滚
                logger.error("发生错误 %s" % result)
                logger.error(sql)
                self.conn.rollback()

    def save_ocop_detail_data(self, datas):
        """保存一分一段详细数据

        :param datas:
        :return:
        """
        with OcopDB() as db:
            for data in datas:
                # SQL 插入语句
                sql = """
                        INSERT INTO LQ_YFYD
                          (LQNF, SYDSFMC, KLMC, FSSX, FSXX, BFSDRS, BFSDLJRS)
                        VALUES
                          ({}, '{}', '{}', {}, {}, {}, {})
                        """.format(data["lqnf"], data["sydsfmc"],
                                   data["klmc"], data["fssx"],
                                   data["fsxx"], data["bfsdrs"],
                                   data["bfsdljrs"])
                try:
                    # 执行sql语句
                    db.execute(sql)
                except Exception as result:
                    # 发生错误时回滚
                    logger.error("发生错误 %s" % result)
                    logger.error(sql)
                    self.conn.rollback()

    def get_ocop_base_count(self, sydsfmc, lqnf, klmc):
        """查看一分一段基本数据是否存在

        :param sydsfmc:
        :param lqnf:
        :param klmc:
        :return:
        """
        with OcopDB() as db:
            klmc_param = "" if klmc == "" or klmc is None else " and klmc = '%s'" % klmc
            sql = """
                    SELECT COUNT(1) count
                      FROM LQ_YFYDWLDZ
                     WHERE SYDSFMC = '{}'
                       AND LQNF = {}
                       {}
                    """.format(sydsfmc, lqnf, klmc_param)
            # 使用 execute()  方法执行 SQL 查询
            db.execute(sql)
            # 使用 fetchone() 方法获取单条数据.
            result = db.fetchone()
            return int(result[0])

    def get_ocop_detail_count(self, sydsfmc, lqnf, klmc):
        """查看一分一段详细数据是否存在

        :param sydsfmc:
        :param lqnf:
        :param klmc:
        :return:
        """
        with OcopDB() as db:
            klmc_param = "" if klmc == "" or klmc is None else " and klmc = '%s'" % klmc
            sql = """
                    SELECT COUNT(1) count
                      FROM LQ_YFYD
                     WHERE SYDSFMC = '{}'
                       AND LQNF = {}
                       {}
                    """.format(sydsfmc, lqnf, klmc_param)
            # 使用 execute()  方法执行 SQL 查询
            db.execute(sql)
            # 使用 fetchone() 方法获取单条数据.
            result = db.fetchone()
            return int(result[0])
