'''
Created on 1 Dec 2014

@author: theodojo
'''

from sqlalchemy import types
from utils import getUnixTime


stringTypes = [types.BINARY, types.CHAR, types.NCHAR, types.NVARCHAR,
               types.TEXT, types.VARBINARY, types.VARCHAR]
integerTypes = [types.BIGINT, types.INTEGER, types.SMALLINT]
floatTypes = [types.FLOAT]
doubleTypes = [types.REAL, types.DECIMAL, types.NUMERIC]
dateTypes = [types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP]
booleanTypes = [types.BOOLEAN]
lobTypes = [types.BLOB, types.CLOB]


class sqlTypeHandler(object):
    typeList = []

    @classmethod
    def expFunc(cls, x):
        return x

    @classmethod
    def impFunc(cls, x):
        return x

    @classmethod
    def isObject(cls, typeVal):
        return any([isinstance(typeVal, x) for x in cls.typeList])


class sqlString(sqlTypeHandler):
    typeList = stringTypes


class sqlInteger(sqlTypeHandler):
    typeList = integerTypes

    @classmethod
    def impFunc(cls, x):
        return "toInt(%s)" % x


class sqlFloat(sqlTypeHandler):
    typeList = floatTypes

    @classmethod
    def impFunc(cls, x):
        return "toFloat(%s)" % x


class sqlDouble(sqlFloat):
    typeList = doubleTypes


class sqlDate(sqlInteger):
    typeList = dateTypes

    @classmethod
    def expFunc(cls, x):
        return getUnixTime(x) if x else ''


class sqlBool(sqlTypeHandler):
    typeList = booleanTypes

    @classmethod
    def expFunc(cls, x):
        return 1 if x else 0

    @classmethod
    def impFunc(cls, x):
        return "%s > 0" % x
    expFunc = lambda _, x: 1 if x else 0


class sqlLOB(sqlTypeHandler):
    typeList = lobTypes

    @classmethod
    def expFunc(cls, _):
        return None


def getSubclassesDeep(cls):
    for subcls in cls.__subclasses__():
        for subsubcls in getSubclassesDeep(subcls):
            yield subsubcls
    yield cls


def getHandler(saCol):
    t = saCol['type']
    h = None
    for cls in getSubclassesDeep(sqlTypeHandler):
        if cls.isObject(t):
            h = cls
            break
    else:
        h = sqlTypeHandler
    return h

if __name__ == '__main__':
    for cls in getSubclassesDeep(sqlTypeHandler):
        print cls
