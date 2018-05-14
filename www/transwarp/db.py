#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
数据库连接模块
"""

import threading
import functools
# 数据库引擎对象
engine = None


class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect

# 实现懒连接数据库


class _LazyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            self.connection = engine.connect()
            return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            self.connection.close()
            self.connection = None

# 持有数据库连接上下文对象


class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return self.connection is not None

    def init(self):
        self.connection = _LazyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()


class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()


class _TransactionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions == 0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    @staticmethod
    def commit():
        global _db_ctx
        try:
            _db_ctx.connection.commit()
        except BaseException:
            _db_ctx.connection.rollback()
            raise

    @staticmethod
    def rollback():
        global _db_ctx
        _db_ctx.connection.rollback()


def connection():
    return _ConnectionCtx()


def transaction():
    return _TransactionCtx()


"""
with_connection decorator
"""


def with_connection(func):
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with connection():
            return func(*args, **kw)
    return _wrapper


'''
with_transaction decorator
'''


def with_transaction(func):
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with transaction():
            return func(*args, **kw)
    return _wrapper

# 数据库连接


class DBError(StandardError):
    pass

# 创建数据库引擎


def create_engine(user, password, database):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('连接已初始化...')
    conn = mysql.connector.connect(user=user, password=password, database=database, use_unicode=True)
    engine = _Engine(conn)


_db_ctx = _DbCtx()  # 全局的threadLocal上下文

"""
查询语句
"""


@with_connection
def select(sql, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


"""
实现一个字典字段转换
"""


class Dict(dict):
    def __init__(self, names, values, **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError('Dict has no attribute %s' % key)


if __name__ == '__main__':

    create_engine('root', 'root', 'dbwl')
    values = select('select *from sys_user where id=?', 5)
    for x in values:
        print x
