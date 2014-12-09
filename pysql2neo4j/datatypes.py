'''
Created on 1 Dec 2014

@author: theodojo
'''

from sqlalchemy.sql import sqltypes
from sqlalchemy.dialects import mysql, oracle, mssql, sybase, postgresql
from utils import getUnixTime, getSubclassesDeep


#NOTE: Feedback is needed for vendor-specific data types
stringTypes = [sqltypes.String, oracle.base.ROWID, oracle.base.INTERVAL,
               mssql.base.UNIQUEIDENTIFIER, postgresql.base.UUID,
               sybase.base.UNIQUEIDENTIFIER, postgresql.base.INET,
               postgresql.base.CIDR, postgresql.base.MACADDR,
               postgresql.base.ARRAY, postgresql.json.JSON,
               postgresql.ranges.INT4RANGE, postgresql.ranges.INT8RANGE,
               postgresql.ranges.NUMRANGE, postgresql.ranges.DATERANGE,
               postgresql.ranges.TSRANGE, postgresql.ranges.TSTZRANGE,
               postgresql.base.INTERVAL, postgresql.base.TSVECTOR,
               postgresql.HSTORE]
integerTypes = [sqltypes.Integer, mysql.base.YEAR, mssql.base.DATETIMEOFFSET,
                postgresql.base.OID]
floatTypes = [sqltypes.Numeric, mssql.base.MONEY, mssql.base.SMALLMONEY,
              sybase.base.MONEY, sybase.base.SMALLMONEY]
dateTypes = [sqltypes.DateTime, sqltypes.Date, sqltypes.Time]
booleanTypes = [sqltypes.Boolean, mysql.base.BIT, mssql.base.BIT,
                postgresql.base.BIT, sybase.base.BIT]
lobTypes = [sqltypes.LargeBinary, sqltypes.BINARY, sqltypes.VARBINARY,
            mysql.base.TINYBLOB, mysql.base.MEDIUMBLOB,
            mysql.base.LONGBLOB, oracle.base.RAW, mssql.base.SQL_VARIANT]


class sqlTypeHandler(object):
    '''Base class for all classes that handle SQL data types
    Attributes:
        typeList: all classes (derived from sqlalchemy.types.TypeEngine)
        that will be handled as this class'''
    typeList = []

    @classmethod
    def expFunc(cls, x):
        '''Export as-is'''
        return x

    @classmethod
    def impFunc(cls, x):
        '''Import as-is'''
        return x

    @classmethod
    def isObject(cls, typeVal):
        '''Checks if a given value is instance of any of the classes
        included in typeList'''
        return any([isinstance(typeVal, x) for x in cls.typeList])


class sqlString(sqlTypeHandler):
    '''Handler for string-like columns
    Attributes:
        typeList: all classes (derived from sqlalchemy.types.TypeEngine)
        that will be handled as string'''
    typeList = stringTypes


class sqlInteger(sqlTypeHandler):
    '''Handler for integer-like columns
    Attributes:
        typeList: all classes (derived from sqlalchemy.types.TypeEngine)
        that will be handled as integer'''
    typeList = integerTypes

    @classmethod
    def impFunc(cls, x):
        '''Import surrounded by toInt function'''
        return "toInt(%s)" % x


class sqlFloat(sqlTypeHandler):
    '''Handler for float-like columns
    Attributes:
        typeList: all classes (derived from sqlalchemy.types.TypeEngine)
        that will be handled as float'''
    typeList = floatTypes

    @classmethod
    def impFunc(cls, x):
        '''Import surrounded by toFloat function'''
        return "toFloat(%s)" % x


class sqlDate(sqlInteger):
    '''Handler for Date-like columns.
    Attributes:
        typeList: all classes (derived from sqlalchemy.types.TypeEngine)
        that will be handled as date'''
    typeList = dateTypes

    @classmethod
    def expFunc(cls, x):
        '''Because Neo4j does not support  date properties, all date
        columns will be exported as an unix time integer, i.e. seconds
        since 1/1/1970'''
        return getUnixTime(x) if x else ''


class sqlBool(sqlTypeHandler):
    '''Handler for boolean-like columns
    Attributes:
        typeList: all classes (derived from sqlalchemy.types.TypeEngine)
        that will be handled as boolean'''
    typeList = booleanTypes

    @classmethod
    def expFunc(cls, x):
        '''Export as 0 or 1'''
        return 1 if x else 0

    @classmethod
    def impFunc(cls, x):
        '''Import as evaluation'''
        return "%s > 0" % x


class sqlLOB(sqlTypeHandler):
    '''Handler for LOB-like columns
    Attributes:
        typeList: all classes (derived from sqlalchemy.types.TypeEngine)
        that will be handled as LOB'''
    typeList = lobTypes

    @classmethod
    def expFunc(cls, _):
        '''Do not export'''
        #MAYBE: Handle LOBs
        return None


def getHandler(saCol):
    '''Return: the appropriate class to handle this column
    (according to its data type)'''
    t = saCol['type']
    h = None
    for cls in getSubclassesDeep(sqlTypeHandler):
        if cls.isObject(t):
            h = cls
            break
    else:
        h = sqlTypeHandler
    return h
