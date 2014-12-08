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

#Decide here what to do with relation types
_transformRelTypes = lambda x: x

if TRANSFORM_REL_TYPES == 'allcaps':
    _transformRelTypes = string.upper


class SqlDbInfo(object):
    '''Handles communication with SQL DB.'''

    def __init__(self):
        '''Constructor'''
        sqldburi = getSqlDbUri()
        connection, inspector = getTestedSQLDatabase(sqldburi)
        self.connection = connection
        self.inspector = inspector
        if TRANSFORM_LABEL == 'capitalize':
            self.labelTransform = self.capitalize
        else:
            self.labelTransform = self.noTransform
        allTables = {tableName: TableInfo(self, tableName) \
                     for tableName in self.inspector.get_table_names()}
        self.tables = allTables
        #Processes below will behave as intended only after all tables
        #have been instantiated
        for tableObject in self.tables.values():
            tableObject._resolveForeignKeys()
            tableObject._setIndexedCols()
        self._ensureUniqRelTypes()

    def _ensureUniqRelTypes(self):
        '''There may be synonym relation types refering to different foreign
        keys or even different many-to-many tables. Here we will ensure that
        this will not happen. IMPORTANT:This should be called only after all
        foreign keys have been instantiated.'''
        #Get all objects that will be imported as relationships
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
                #Silly, but it works
                obj1.relType = obj1.relType + str(i * 2)
                obj2.relType = obj2.relType + str(i * 2 + 1)

    @property
    def tableList(self):
        '''list of tables'''
        return self.tables.values()

    def export(self):
        '''Export all tables'''
        for tblName, tblObject in self.tables.items():
            LOG.info("Exporting %s..." % tblName)
            tblObject.export()

    def capitalize(self, tableName):
        '''Capitalize behaviour of self.labelTransform'''
        return string.capitalize(tableName)

    def noTransform(self, tableName):
        '''Do-nothing behaviour of self.labelTransform'''
        return tableName


class TableInfo(object):
    '''Holds all necessary Table metadata'''

    def __init__(self, sqlDb, tableName):
        '''Constructor.
        Parameters:
            - sqlDb: SqlInfo: the SQL Database
            - tableName: string'''
        #TODO: Check which definition is better (or use both)
        self.isManyToMany = self.isManyToManyLoose
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
        #Fetch foreign keys, keep for later
        self._fKeysSA = self.sqlDb.inspector.get_foreign_keys(self.tableName)

    def _resolveForeignKeys(self):
        '''Links foreign keys to other tables. MUST be called AFTER all
        tables have been instantiated as TableInfo objects'''
        fkeycols = []
        self.fKeys = list()
        for fk in self._fKeysSA:
            self.fKeys.append(ForeignKeyInfo(fk, self))
            fkeycols.extend(fk['constrained_columns'])
        #Find all columns of the table that belong to some foreign key
        fkeycolsUniq = listUnique(fkeycols) if self._fKeysSA else list()
        self.fKeysCols = OrderedDict()
        for name in fkeycolsUniq:
            self.fKeysCols[name] = self.cols[name]
            self.cols[name].isFkCol = True
        #All tables this table refers to
        self.refTables = listUnique([k.refTable for k in self.fKeys])
        #Processes below can not be called earlier than that
        if self.isManyToMany():
            relType = "%s_%s" % (self.fKeys[0].refTable.labelName,
                                 self.fKeys[1].refTable.labelName)
            #relType attribute is set ONLY for such tables
            self.relType = _transformRelTypes(relType)
        if REMOVE_REDUNDANT_FIELDS:
            self.importCols = {k: v for k, v in self.cols.items() \
                          if not v.isRedundant()}
        else:
            self.importCols = self.cols

    def _setIndexedCols(self):
        '''Decides which fields must be indexed, based on key and index
        information.'''
        #Find all columns that are indexed or unique indexed
        uniq = self.sqlDb.inspector.get_unique_constraints(self.tableName)
        idx = self.sqlDb.inspector.get_indexes(self.tableName)
        #Only single-field constraints will be carried over as such
        uniqCols = [x['column_names'] for x in uniq \
                    if len(x['column_names']) == 1]
        idxCols = [x['column_names'] for x in idx]
        idxCols.extend([x['column_names'] for x in uniq \
                        if len(x['column_names']) != 1])
        #Don't forget primary key constraint
        if len(self.pkCols) == 1:
            uniqCols.append(self.pkCols.keys())
        else:
            idxCols.append(self.pkCols.keys())

        uniqColNames = listUnique(listFlatten(uniqCols))
        #Remove unique constrained columns from columns to be indexed
        idxColNames = listSubtract(listUnique(listFlatten(idxCols)),
                                    uniqColNames)
        self.uniqCols = [self.cols[x] for x in uniqColNames]
        #Redundant fields are excluded
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
        '''Fetch rows of table'''
        for r in self.sqlDb.connection.execute(self.query):
            yield r

    def export(self):
        '''Exports table to a csv file'''
        #Send header first
        header = [x for x in self.cols.keys()]
        csvFileWriter = CsvHandler(self.tableName, header)
        for rowData in self.iterRows():
            csvData = [v.expFunc(rowData[k]) \
                       for k, v in self.cols.items()]
            csvFileWriter.writeRow(csvData)
        csvFileWriter.close()
        self.filesWritten = csvFileWriter.getFilesWritten()

    def hasCompositePK(self):
        '''True if this table has a composite primary key'''
        return len(self.pkCols) > 1

    def hasPK(self):
        '''True if this table has a primary key'''
        return len(self.pkCols) > 0

    def hasFkeys(self):
        '''True if this table has foreign keys'''
        return len(self.fKeys) > 0

    def isManyToManyStrict(self):
        '''True if this table implements a many-to-many relationship.
        Definition used:
            - Referring two tables.
            - Primary key composed exclusively out of foreign keys
            - There are no tables referring to this table'''
        return len(self.refTables) == 2 and \
            len(listSubtract(self.pkCols.keys(), self.fKeysCols.keys())) == 0 \
            and len(self.depTables) == 0

    def isManyToManyLoose(self):
        '''True if this table implements a many-to-many relationship.
        Definition used:
            - Referring two tables.
            - There are no tables referring to this table'''
        return len(self.refTables) == 2 and \
            len(self.depTables) == 0

    def asNodeInfo(self):
        '''Returns necessary info to create a node out of this table metadata.
        If this is a many-to-many table, returns None.'''
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
        '''Returns necessary info to create a relationship out of this table
        metadata. If this is NOT a many-to-many table, returns None.'''
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
    '''Holds all necessary Column metadata'''
    def __init__(self, saCol, table):
        '''Constructor.
        Parameters:
            - saCol: column metadata as returned from SQLAlchemy
            - table: tableInfo object of column's table'''
        self.table = table
        self.name = saCol['name']
        self.__handler = getHandler(saCol)
        self.expFunc = lambda x: self.__handler.expFunc(x)
        self.impFunc = lambda x: self.__handler.impFunc(x)
        self.isPkCol = None
        self.isFkCol = False

    def isRedundant(self):
        '''Returns true if this column is a redundant one. Makes sense
        to call it after all foreign keys have been instantiated.
        Definition:
            - Belongs to a foreign key
            - Does not belong to table's primary key (not applicable to
            many-to-many tables)'''
        if MANY_TO_MANY_AS_RELATION and self.table.isManyToMany():
            return self.isFkCol
        else:
            return self.isFkCol and (not self.isPkCol)


class ForeignKeyInfo(object):
    '''Holds all necessary Foreign Key metadata'''
    def __init__(self, fKeyConstr, table):
        '''Constructor.
        Parameters
            - fKeyConstr: foreign key metadata as returned from SQLAlchemy
            - table: tableInfo object of foreign key's table'''
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
        '''Returns necessary info to create a relationship out of this foreign
        key metadata.'''
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
