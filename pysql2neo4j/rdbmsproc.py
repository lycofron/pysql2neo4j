'''
Created on 04 May 2013

@author: theodojo
'''

import string
from collections import OrderedDict
from itertools import combinations

from sqlalchemy import create_engine, MetaData, select
from sqlalchemy.engine import reflection
from sqlalchemy import Table, Column, Integer

from csvproc import CsvHandler
from utils import listUnique, listSubtract, listFlatten
from customexceptions import DBInsufficientPrivileges, DbNotFoundException
from customexceptions import DBUnreadableException
from datatypes import getHandler
from configman import getSqlDbUri, TRANSFORM_LABEL, TRANSFORM_REL_TYPES
from configman import LOG, DRY_RUN, MANY_TO_MANY_AS_RELATION
from configman import REMOVE_REDUNDANT_FIELDS

_transformRelTypes = lambda x: x

if TRANSFORM_REL_TYPES == 'allcaps':
    _transformRelTypes = string.upper


class SqlDbInfo(object):
    def __init__(self):
        sqldburi = getSqlDbUri()
        connection, inspector = getTestedSQLDatabase(sqldburi)
        self.connection = connection
        self.inspector = inspector
        labelTransform = TRANSFORM_LABEL
        if labelTransform == 'capitalize':
            self.labelTransform = self.capitalize
        else:
            self.labelTransform = self.noTransform
        allTables = {tableName: TableInfo(self, tableName) \
                     for tableName in self.inspector.get_table_names()}
        self.tables = allTables
        for tableObject in self.tables.values():
            tableObject._resolveForeignKeys()
            tableObject._setIndexedCols()
        self._ensureUniqRelTypes()

    def _ensureUniqRelTypes(self):
        '''There may be synonym relation types refering to different foreign
        keys or even different many-to-many tables. Here we will ensure that
        this will not happen'''
        m2mTables = [tbl for tbl in self.tables.values() \
                     if tbl.isManyToMany()]
        allFKeys = [tbl.fKeys for tbl in self.tables.values() \
                     if not tbl.isManyToMany()]
        allRelObjects = listFlatten(allFKeys)
        allRelObjects.extend(m2mTables)
        for i, (obj1, obj2) in enumerate(combinations(allRelObjects, 2)):
            assert hasattr(obj1, 'relType') and \
                hasattr(obj2, 'relType')
            if obj1.relType == obj2.relType:
                #Silly, but it should work
                obj1.relType = obj1.relType + str(i * 2)
                obj2.relType = obj2.relType + str(i * 2 + 1)

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
        self.depTables = list()
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
        fkeycolsUniq = listUnique(fkeycols) if self._fKeysSA else list()
        self.fKeysCols = OrderedDict()
        for name in fkeycolsUniq:
            self.fKeysCols[name] = self.cols[name]
            self.cols[name].isFkCol = True
        self.refTables = [k.refTable for k in self.fKeys]
        if self.isManyToMany():
            relType = "%s_%s" % (self.fKeys[0].refTable.labelName,
                                 self.fKeys[1].refTable.labelName)
            self.relType = _transformRelTypes(relType)
        if REMOVE_REDUNDANT_FIELDS:
            self.importCols = {k: v for k, v in self.cols.items() \
                          if not v.isRedundant()}
        else:
            self.importCols = self.cols

    def _setIndexedCols(self):
        uniq = self.sqlDb.inspector.get_unique_constraints(self.tableName)
        idx = self.sqlDb.inspector.get_indexes(self.tableName)
        uniqCols = [x['column_names'] for x in uniq \
                    if len(x['column_names']) == 1]
        idxCols = [x['column_names'] for x in idx]
        idxCols.extend([x['column_names'] for x in uniq \
                        if len(x['column_names']) != 1])
        if len(self.pkCols) == 1:
            uniqCols.append(self.pkCols.keys())
        else:
            idxCols.append(self.pkCols.keys())

        uniqColNames = listUnique(listFlatten(uniqCols))
        idxColNames = listSubtract(listUnique(listFlatten(idxCols)),
                                    uniqColNames)
        self.uniqCols = [self.cols[x] for x in uniqColNames]
        if REMOVE_REDUNDANT_FIELDS:
            self.idxCols = [self.cols[x] for x in idxColNames \
                            if not self.cols[x].isRedundant()]
        else:
            self.idxCols = [self.cols[x] for x in idxColNames]
        LOG.debug("Unique constraints on table %s, columns %s" %
                  (self.tableName, str([x.name for x in self.uniqCols])))
        LOG.debug("Indexes on table %s, columns %s" %
                  (self.tableName, str([x.name for x in self.idxCols])))

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

    def isManyToMany(self):
        return len(self.refTables) == 2 and \
            len(listSubtract(self.pkCols.keys(), self.fKeysCols.keys())) == 0 \
            and len(self.depTables) == 0

    def asNodeInfo(self):
        if self.isManyToMany():
            return None
        else:
            labels = ["Pysql2neo4j", "SchemaInfo"]
            cols = {c: self.labelName for c in self.importCols.keys()}
            # Hope you don't have any table
            # with a field named __tablename
            cols["__tablename"] = self.labelName
            return labels, cols

    def asRelInfo(self):
        if self.isManyToMany():
            srcRefCols = self.fKeys[0].refCols.keys()
            destRefCols = self.fKeys[1].refCols.keys()
            srcRefColsFQ = [self.fKeys[0].refTable.labelName + "." + i
                            for i in srcRefCols]
            destRefColsFQ = [self.fKeys[1].refTable.labelName + "." + i
                             for i in destRefCols]
            properties = {k: v for k, v in zip(srcRefColsFQ, destRefColsFQ)}
            properties['__relationType'] = self.relType
            return self.fKeys[0].refTable.labelName, self.relType, \
                    self.fKeys[1].refTable.labelName, properties
        else:
            return None


class ColumnInfo(object):
    def __init__(self, saCol, table):
        self.table = table
        self.name = saCol['name']
        self.__handler = getHandler(saCol)
        self.expFunc = lambda x: self.__handler.expFunc(x)
        self.impFunc = lambda x: self.__handler.impFunc(x)
        self.isPkCol = None
        self.isFkCol = False

    def isRedundant(self):
        if MANY_TO_MANY_AS_RELATION and self.table.isManyToMany():
            return self.isFkCol
        else:
            return self.isFkCol and (not self.isPkCol)


class ForeignKeyInfo(object):
    def __init__(self, fKeyConstr, table):
        self.table = table
        self.refTable = self.table.sqlDb.tables[fKeyConstr['referred_table']]
        self.refTable.depTables.append(self.table)
        self.consCols = OrderedDict()
        for colName in fKeyConstr['constrained_columns']:
            self.consCols[colName] = self.table.cols[colName]
        self.refCols = OrderedDict()
        for colName in fKeyConstr['referred_columns']:
            self.refCols[colName] = self.refTable.cols[colName]
        relType = "%s_%s" % (self.refTable.labelName, self.table.labelName)
        self.relType = _transformRelTypes(relType)

    def asRelInfo(self):
        if self.table.isManyToMany():
            return None
        else:
            srcRefColsFQ = [self.refTable.labelName + "." + i
                            for i in self.refCols.keys()]
            destRefColsFQ = [self.table.labelName + "." + i
                            for i in self.consCols.keys()]
            properties = {k: v for k, v in zip(srcRefColsFQ, destRefColsFQ)}
            properties['__relationType'] = self.relType
            return self.refTable.labelName, self.relType, \
                    self.table.labelName, properties


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
    if not DRY_RUN:
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


def m2mWithSameRef(tbl1, tbl2):
    assert len(tbl1.fKeys) == 2 and len(tbl2.fKeys) == 2
    return tbl1 != tbl2 and \
        ((tbl1.fKeys[0] == tbl2.fKeys[0] \
          and tbl1.fKeys[1] == tbl2.fKeys[1]) or
         (tbl1.fKeys[0] == tbl2.fKeys[1] \
          and tbl1.fKeys[1] == tbl2.fKeys[0]))
