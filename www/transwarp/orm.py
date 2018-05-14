#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
ORM模块
"""
import db


class Field(object):
    def __init__(self, name, column_type):
        self.name = name
        self.column_type = column_type

    def __str__(self):
        print '<%s:%s>' % (self.__class__.__name__, self.name)


class StringField(Field):
    def __init__(self, name):
        super(StringField, self).__init__(name, 'varchar(100)')


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        print('Found model: %s' % name)
        mappings = dict()
        for key, value in attrs.iteritems():
            if isinstance(value, Field):
                mappings[key] = value
        for k in mappings.iterkeys():
            attrs.pop(k)
        attrs['__mappings__'] = mappings
        return type.__new__(cls, name, bases, attrs)


# 所有model的父类


class Model(dict):
    __metaclass__ = ModelMetaclass

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __setattr__(self, key, value):
        self[key] = value

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def save(self):
        fields = []
        params = []
        args = []
        for k, v in self.__mappings__.iteritems():
            fields.append(v.name)
            params.append('?')
            args.append(getattr(self, k, None))
        sql = 'insert into %s(%s) values(%s)' % (self.__table__, ','.join(fields), ','.join(params))
        print sql
        print 'args: %s' % ','.join(args)
        db.create_engine('root', 'root', 'dbwl')
        return db.update(sql, *args)


class UserModel(Model):
    __table__ = 'sys_user'
    id = StringField('id')
    name = StringField('name')


if __name__ == '__main__':
    user = UserModel(id='5', name='wangli')
    print user.save()

