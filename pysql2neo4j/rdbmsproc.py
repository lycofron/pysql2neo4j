'''
Created on 04 May 2013

@author: theodojo
'''

import string
from collections import OrderedDict

from sqlalchemy import create_engine, MetaData, select
from sqlalchemy.engine import reflection
from sqlalchemy import Table, Column, Integer

from configman import getSqlDbUri, confDict, LOG
from csvproc import CsvHandler
from utils import listUnique
from customexceptions import DBInsufficientPrivileges, DbNotFoundException
from customexceptions import DBUnreadableException
from datatypes import getHandler

_transformRelTypes = lambda x: x

if confDict['transformRelTypes'] == 'allcaps':
    _transformRelTypes = string.upper


class SqlDbInfo(object):
    def __init__(self):
        sqldburi = getSqlDbUri()
        connection, inspector = getTestedSQLDatabase(sqldburi)
        self.connection = connection
        self.inspector = inspector
        labelTransform = confDict['labeltransform']
        if labelTransform == 'capitalize':
            self.labelTransform = self.capitalize
        else:
            self.labelTransform = self.noTransform
        allTables = {tableName: TableInfo(self, tableName) \
                     for tableName in self.inspector.get_table_names()}
        self.tables = allTables
        for tableObject in self.tables.values():
            tableObject._resolveForeignKeys()

    @property
    def iterTables(self):
        return self.tables.values()

    def export(self):
        for tblName, tblObject in self.tables.items():
            LOG.info("Exporting %s..." % tblName)
            tblObject.export()

    def capitalize(self, tableName):
        return string.capitalize(tableName)

    def noTransform(self, tableName):
        return tableName


class TableInfo(object):

    def __init__(self, sqlDb, tableName):
        meta = MetaData()
        self.sqlDb = sqlDb
        saTableMetadata = Table(tableName, meta)
        self.sqlDb.inspector.reflecttable(saTableMetadata, None)
        self.query = select([saTableMetadata])
        self.tableName = saTableMetadata.name
        self.labelName = self.sqlDb.labelTransform(self.tableName)
        columns = self.sqlDb.inspector.get_columns(self.tableName)
        self.cols = OrderedDict()
        for x in columns:
            self.cols[x['name']] = ColumnInfo(x, self)
        pk = self.sqlDb.inspector.get_pk_constraint(self.tableName)
        pkCols = pk["constrained_columns"]
        self.pkCols = OrderedDict()
        for name, col in self.cols.items():
            if name in pkCols:
                self.pkCols[name] = col
                col.isPkCol = True
            else:
                col.isPkCol = False
        self._fKeysSA = self.sqlDb.inspector.get_foreign_keys(self.tableName)

    def _resolveForeignKeys(self):
        fkeycols = []
        self.fKeys = list()
        for fk in self._fKeysSA:
            self.fKeys.append(ForeignKeyInfo(fk, self))
            fkeycols.extend(fk['constrained_columns'])
        self.fKeysCols = listUnique(fkeycols) if self._fKeysSA else list()
        self.refTables = [k.refTable for k in self.fKeys]

    def iterRows(self):
        for r in self.sqlDb.connection.execute(self.query):
            yield r

    def export(self):
        header = [x for x in self.cols.keys()]
        csvFileWriter = CsvHandler(self.tableName, header)
        for rowData in self.iterRows():
            csvData = [v.expFunc(rowData[k]) \
                       for k, v in self.cols.items()]
            csvFileWriter.writeRow(csvData)
        csvFileWriter.close()
        self.filesWritten = csvFileWriter.getFilesWritten()

    def hasCompositePK(self):
        return len(self.pkCols) > 1

    def hasPK(self):
        return len(self.pkCols) > 0

    def hasFkeys(self):
        return len(self.fKeys) > 0


class ColumnInfo(object):
    def __init__(self, saCol, table):
        self.table = table
        self.name = saCol['name']
        self.__handler = getHandler(saCol)
        self.expFunc = lambda x: self.__handler.expFunc(x)
        self.impFunc = lambda x: self.__handler.impFunc(x)
        self.isPKeyCol = None


class ForeignKeyInfo(object):
    def __init__(self, fKeyConstr, table):
        self.table = table
        self.refTable = self.table.sqlDb.tables[fKeyConstr['referred_table']]
        self.consCols = OrderedDict()
        for colName in fKeyConstr['constrained_columns']:
            self.consCols[colName] = self.table.cols[colName]
        self.refCols = OrderedDict()
        for colName in fKeyConstr['referred_columns']:
            self.refCols[colName] = self.refTable.cols[colName]
        relType = "%s_%s" % (self.refTable.labelName, self.table.labelName)
        self.relType = _transformRelTypes(relType)


def getTestedSQLDatabase(dburi, tryWrite=False):
    '''Gets an sqlalchemy db uri and returns a triplet of engine, connection
       and inspector after testing adequately that the database is functional.
       If tryWrite is True, it will test for table creation, insert,
       update and delete.'''
    try:
        engine = create_engine(dburi)
        conn = engine.connect()
        insp = reflection.Inspector.from_engine(engine)
    except Exception as ex:
        raise DbNotFoundException(ex, "Could not connect to SQL DB %s."
                                  % dburi)
    try:
        meta = MetaData()
        meta.reflect(bind=engine)
        sampleTblName = insp.get_table_names()[0]
        sampleTbl = Table(sampleTblName, meta)
        #insp.reflecttable(sampleTbl,None)
        s = select([sampleTbl])
        result = conn.execute(s)
        _ = result.fetchone()
    except Exception as ex:
        raise  DBUnreadableException(ex, "Could not SELECT on SQL DB %s."
                                     % dburi)
    if tryWrite:
        try:
            md = MetaData()
            testTable = Table('example', md,
                              Column('id', Integer, primary_key=True))
            md.create_all(engine)
        except Exception as ex:
            raise DBInsufficientPrivileges(ex,
                                           "Failed to create table in DB %s ."
                                           % dburi)

        try:
            ins = testTable.insert().values(id=1)
            _ = conn.execute(ins)
            s = select([testTable])
            _ = conn.execute(s)
            stmt = testTable.update().values(id=2)
            conn.execute(stmt)
            conn.execute(testTable.delete())
            testTable.drop(bind=engine)
        except Exception as ex:
            raise DBInsufficientPrivileges(\
                    "Failed on trivial operations in DB %s." % dburi)
    return conn, insp
