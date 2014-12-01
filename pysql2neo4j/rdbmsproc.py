'''
Created on 04 May 2013

@author: theodojo
'''

import string

from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy import create_engine, MetaData, select
from sqlalchemy.engine import reflection
from sqlalchemy import Table, Column, Integer

from configman import getSqlDbUri, confDict
from csvproc import CsvHandler
from utils import listUnique
from customexceptions import DBInsufficientPrivileges, DbNotFoundException
from customexceptions import DBUnreadableException, WorkflowException


class SqlDbInfo(object):
    def __init__(self):
        sqldburi = getSqlDbUri()
        connection, inspector = getTestedSQLDatabase(sqldburi)
        labelTransform = confDict['labeltransform']
        allTables = list()
        for t in inspector.get_table_names():
            meta = MetaData()
            tblMeta = Table(t, meta)
            allTables.append(TableInfo(tblMeta, connection, inspector, labelTransform))
        for t in allTables:
            t._resolveForeignKeys(allTables)
        self.tables = allTables

    @property
    def iterTables(self):
        return self.tables

    def export(self):
        for table in self.tables:
            print "Exporting %s..." % table.tablename
            table.export()


class TableInfo(object):

    def __init__(self, saTableMetadata, connection, inspector, labelTransform):
        self.__connection = connection
        inspector.reflecttable(saTableMetadata, None)
        self.query = select([saTableMetadata])
        self.tablename = saTableMetadata.name
        if labelTransform == 'capitalize':
            self.labelName = string.capitalize(self.tablename)
        else:
            self.labelName = self.tablename
        columns = inspector.get_columns(self.tablename)
        self.allcols = [x["name"] for x in columns]
        constraints = inspector.get_pk_constraint(self.tablename)
        self.pkeycols = constraints["constrained_columns"]
        self._fKeys = [x for x in saTableMetadata.constraints if type(x) == ForeignKeyConstraint]
        fkeycols = []
        for x in self._fKeys:
            fkeycols.extend(x.columns)
        self.fKeysCols = listUnique(fkeycols) if self._fKeys else list()
        self.essCols = [x for x in self.allcols if x not in self.fKeysCols]

    def _resolveForeignKeys(self, dbContext):
        self.foreignKeys = list()
        for t in self._fKeys:
            self.foreignKeys.append(ForeignKeyInfo(t, self, dbContext))
        self.refTables = [k.refTable for k in self.foreignKeys]

    def iterRows(self):
        for r in self.__connection.execute(self.query):
            yield r

    def export(self):
        csvFileWriter = CsvHandler(self)
        for rowData in self.iterRows():
            csvFileWriter.writeRow(list(rowData))
        csvFileWriter.close()
        self.filesWritten = csvFileWriter.getFilesWritten()


class ForeignKeyInfo(object):
    def __init__(self, fKeyConstr, table, dbContext):
        self._metadata = fKeyConstr
        self.table = table
        assert len(fKeyConstr.elements) > 0
        refTblNames = list(set([elem.column.table.name
                                      for elem in fKeyConstr.elements]))
        assert len(refTblNames) == 1
        refTablesAll = [t for t in dbContext if t.tablename == refTblNames[0]]
        assert len(refTablesAll) == 1
        self.refTable = refTablesAll[0]
        self.cols = []
        for i in zip(fKeyConstr.columns, [elem.column.name
                                            for elem in fKeyConstr.elements]):
            self.cols.append({"referencing": i[0], "referenced": i[1]})


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
        raise DbNotFoundException(ex, "Could not connect to DB %s." % dburi)
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
        raise  DBUnreadableException(ex, "Could not SELECT on DB %s." % dburi)
    if tryWrite:
        if insp.get_table_names():
            raise WorkflowException("DB %s is not empty." % dburi)

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
            raise DBInsufficientPrivileges("Exception while testing trivial operations in DB %s."
                                           % dburi)
    return conn, insp
