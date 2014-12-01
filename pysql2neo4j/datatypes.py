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
    expFunc = lambda _, x: x
    impFunc = lambda _, x: x

    @classmethod
    def isObject(cls, typeVal):
        return any([isinstance(typeVal, x) for x in cls.typeList])


class sqlString(sqlTypeHandler):
    typeList = stringTypes


class sqlInteger(sqlTypeHandler):
    typeList = integerTypes
    impFunc = lambda _, x: "toInt(%s)" % x


class sqlFloat(sqlTypeHandler):
    typeList = floatTypes
    impFunc = lambda _, x: "toFloat(%s)" % x


class sqlDouble(sqlTypeHandler):
    typeList = doubleTypes
    impFunc = lambda _, x: "toFloat(%s)" % x


class sqlDate(sqlTypeHandler):
    typeList = dateTypes
    expFunc = lambda _, x: getUnixTime(x) if x else ''
    impFunc = lambda _, x: "toInt(%s)" % x


class sqlBool(sqlTypeHandler):
    typeList = booleanTypes
    expFunc = lambda _, x: 1 if x else 0
    impFunc = lambda _, x: "%s > 0" % x


class sqlLOB(sqlTypeHandler):
    typeList = lobTypes
    expFunc = lambda *unused: None


def getHandler(saCol):
    t = saCol['type']
    h = None
    for cls in sqlTypeHandler.__subclasses__():
        if cls.isObject(t):
            h = cls()
            break
    else:
        h = sqlTypeHandler()
    return h
