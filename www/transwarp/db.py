#!/usr/bin/python
#-*- coding:utf-8 -*-
'''
数据库连接模块
'''
import threading
import functools
#数据库引擎对象
engine = None

class _Engine(object):
    def __init__(self,connect):
        self._connect = connect
    def connect(self):
        return self._connect

#实现懒连接数据库
class _LasyConnection(object):
    def __init__(self):
        self.connectoin=None

    def cursor(self):
        if self.connectoin is None:
            self.connectoin=engine.connect()
    def commit(self):
        self.connectoin.commit()

    def rollback(self):
        self.connectoin.rollback()

    def cleanup(self):
        if self.connection:
            self.connection.close()
            self.connection=None

#持有数据库连接上下文对象
class _DbCtx(threading.local):
    def __init__(self):
        self.conection=None
        self.transactions=0

    def is_init(self):
        return not self.conection is None

    def init(self):
        self.conection=_LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()


class _ConectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup=False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup=True
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
            if _db_ctx.transactions==0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        try:
            _db_ctx.connection.commit()
        except:
            _db_ctx.connection.rollback()
            raise

    def rollback(self):
        global _db_ctx
        _db_ctx.connection.rollback()


def connection():
    return _ConectionCtx()

def transaction():
    return _TransactionCtx()

'''
with_connecton decorator
'''
def with_connection(func):
    @functools.wraps(func)
    def _wrapper(*args,**kw):
        with connection():
            return func(*args, **kw)
    return _wrapper

''''
with_transaction decorator
'''
def with_transaction(func):
    @functools.wraps(func)
    def _wrapper(*args,**kw):
        with transaction():
            return func(*args,**kw)
    return _wrapper

#数据库连接
class DBError(StandardError):
    pass

#创建数据库引擎
def create_engine(user,password,database):
    import mysql.connector
    global engine
    if not engine is None:
        raise DBError('连接已初始化...')
    conn=mysql.connector.connect(user, password, database, use_unicode=True)
    engine=_Engine(conn)

_db_ctx=_DbCtx()

"""
查询语句
"""
@with_connection
def select(sql,*args):
    global _db_ctx
    cursor=None
    sql=sql.replace('?','%s')
    try:
        cursor=_db_ctx.connection.cursor()
        cursor.execute(sql,args)
        return cursor.fetchall()
    finally:
        if cursor:
            cursor.close()
