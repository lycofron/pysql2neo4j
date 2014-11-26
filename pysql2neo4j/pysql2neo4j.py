'''
Created on 04 May 2013

@author: theodojo
'''
from sqlalchemy import select, MetaData ,create_engine, Table 
from sqlalchemy.engine import reflection
from sqlalchemy.schema import ForeignKeyConstraint
#from py2neo import neo4j

from configman import DBConnManager

class SqlDbProcessor(object):
    def __init__(self):
        wrkDB = DBConnManager()
        self.insp = reflection.Inspector.from_engine(wrkDB.srcengine)
        allTables = list()
        for t in self.insp.get_table_names():
            meta = MetaData()
            allTables.append(TableProcessor(Table(t,meta)))
#         self.tables = list()
#         while allTables:
#             allTableNames = [t.tablename for t in self.tables]
#             unRefTables = [t for t in allTables if (len(t._referenceTables)==0 or all([x.name in allTableNames for x in t._referenceTables]))]
#             assert not (len(unRefTables)==0 and len(allTables)>0)
#             self.tables.extend(unRefTables)
#             allTables=[t for t in allTables if t not in unRefTables]
        for t in allTables:
            t._resolveForeignKeys(allTables)      
        self.tables = allTables
        

    @property
    def iterTables(self):
        return self.tables

class TableProcessor(object):
    
    def __init__(self,saTableMetadata):
        self.metadata = saTableMetadata
        self.wrkDB = DBConnManager()
        self.wrkDB.srcinsp.reflecttable(saTableMetadata,None)
        self.query = select([saTableMetadata])
        self.tablename = saTableMetadata.name
        self.allcols = [x["name"] for x in self.wrkDB.srcinsp.get_columns(self.tablename)]
        self.pkeycols = self.wrkDB.srcinsp.get_pk_constraint(self.tablename)["constrained_columns"]
        self._fKeys = [x for x in saTableMetadata.constraints if type(x)==ForeignKeyConstraint]
        self._referenceTables = list(set([fkey.column.table for elements in [x.elements for x in self._fKeys] for fkey in elements]))
        self.fKeysCols = self._fKeys and [col for fKeyCols in [x.columns for x in self._fKeys] for col in fKeyCols] or list()
        self.essCols = [x for x in self.allcols if x not in self.fKeysCols]

    def iterRows(self):
        for r in self.wrkDB.srcconn.execute(self.query):
            yield r
    
    def _resolveForeignKeys(self, dbContext):
        self.foreignKeys=list()
        for t in self._fKeys:
            self.foreignKeys.append(ForeignKeyProcessor(t, dbContext))
        self.refTables = [k.refTable for k in self.foreignKeys]

class ForeignKeyProcessor(object):
    def __init__(self, foreignKeyConstraint, dbContext):
        self.metadata = foreignKeyConstraint
        assert len(foreignKeyConstraint.elements)>0
        refTablesAllNames = list(set([elem.column.table.name for elem in foreignKeyConstraint.elements]))
        assert len(refTablesAllNames)==1
        refTablesAll=[t for t in dbContext if t.tablename==refTablesAllNames[0]]
        assert len(refTablesAll)==1
        self.refTable = refTablesAll[0]
        self.cols = []
        for i in zip(foreignKeyConstraint.columns,[elem.column.name for elem in foreignKeyConstraint.elements]):
            self.cols.append({"referencing":i[0],"referenced":i[1]})
