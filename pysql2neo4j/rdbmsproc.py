'''
Created on 04 May 2013

@author: theodojo
'''
from sqlalchemy import select, MetaData, Table
from sqlalchemy.engine import reflection
from sqlalchemy.schema import ForeignKeyConstraint

from configman import DBConnManager
from csvproc import CsvHandler
from utils import listUnique


class SqlDbProcessor(object):
    def __init__(self):
        wrkDB = DBConnManager()
        self.insp = reflection.Inspector.from_engine(wrkDB.srcengine)
        allTables = list()
        for t in self.insp.get_table_names():
            meta = MetaData()
            allTables.append(TableProcessor(Table(t, meta)))
#         self.tables = list()
#         while allTables:
#             allTableNames = [t.tablename for t in self.tables]
#             unRefTables = [t for t in allTables
#                              if (len(t._referenceTables)==0
#                              or all([x.name in allTableNames for x in t._referenceTables]))]
#             assert not (len(unRefTables)==0 and len(allTables)>0)
#             self.tables.extend(unRefTables)
#             allTables=[t for t in allTables if t not in unRefTables]
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


class TableProcessor(object):

    def __init__(self, saTableMetadata):
        self.metadata = saTableMetadata
        self.wrkDB = DBConnManager()
        inspector = self.wrkDB.srcinsp
        inspector.reflecttable(saTableMetadata, None)
        self.query = select([saTableMetadata])
        self.tablename = saTableMetadata.name
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
            self.foreignKeys.append(ForeignKeyProcessor(t, self, dbContext))
        self.refTables = [k.refTable for k in self.foreignKeys]

    def iterRows(self):
        for r in self.wrkDB.srcconn.execute(self.query):
            yield r

    def export(self):
        csvFileWriter = CsvHandler(self)
        for rowData in self.iterRows():
            csvFileWriter.writeRow(list(rowData))
        csvFileWriter.close()
        self.filesWritten = csvFileWriter.getFilesWritten()


class ForeignKeyProcessor(object):
    def __init__(self, fKeyConstr, table, dbContext):
        self.metadata = fKeyConstr
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
