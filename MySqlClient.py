# coding=utf-8
"""
使用DBUtils数据库连接池中的连接，操作数据库
OperationalError: (2006, ‘MySQL server has gone away’)
"""
import traceback

import pymysql
from dbutils.pooled_db import PooledDB
from loguru import logger

pymysql.install_as_MySQLdb()


class MySqlClient(object):
    __pool = None

    def __init__(self,
                 creator=pymysql,  # 使用链接数据库的模块
                 maxconnections=6,  # 连接池允许的最大连接数，0和None表示不限制连接数
                 mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                 maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
                 maxshared=3,
                 # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，
                 # 所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
                 blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                 maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
                 setsession=None,  # 开始会话前执行的命令列表。
                 ping=0,
                 # ping MySQL服务端，检查是否服务可用。
                 host='127.0.0.1',
                 port=3306,
                 user='root',
                 password='HuiShiWei::37013',
                 database='video_player3',
                 charset='utf8'):
        super().__init__()
        if setsession is None:
            setsession = []
        if not self.__pool:
            self.__pool = PooledDB(
                creator=creator, maxconnections=maxconnections, mincached=mincached, maxcached=maxcached,
                maxshared=maxshared, blocking=blocking, maxusage=maxusage, setsession=setsession, ping=ping, host=host,
                port=port, user=user, password=password, database=database, charset=charset
            )

    @staticmethod
    def close(_cursor, _conn):
        try:
            _cursor.close()
            _conn.close()
        except Exception as e:
            logger.error(f"close mysql error {e}")
            traceback.print_exc()

    def select_many(self, sql, param=()):
        """
        查询多个结果
        :param sql: qsl语句
        :param param: sql参数
        :return: 结果数量和查询结果集
        """
        _conn = self.__pool.connection()
        _cursor = _conn.cursor()
        count = _cursor.execute(sql, param)
        result = _cursor.fetchall()
        self.close(_conn, _cursor)
        return count, result

    def insert(self, sql, param=()):
        _conn = self.__pool.connection()
        _cursor = _conn.cursor()
        try:
            _cursor.execute(sql, param)
        except Exception as e:
            logger.error(f"{param} {e}")
            # traceback.print_exc()
        _id = _cursor.lastrowid
        _conn.commit()
        self.close(_conn, _cursor)
        return _id

    def insert_batch(self, sql, param=()):
        _conn = self.__pool.connection()
        _cursor = _conn.cursor()
        try:
            _cursor.executemany(sql, param)
        except Exception as e:
            logger.error(f"{param} {e}")
            # traceback.print_exc()
        _id = _cursor.lastrowid
        _conn.commit()
        self.close(_conn, _cursor)
        return _id
