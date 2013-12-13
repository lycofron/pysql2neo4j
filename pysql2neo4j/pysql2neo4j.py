'''
Created on 04 May 2013

@author: theodojo
'''

from sqlalchemy import select #,create_engine, MetaData, Table 
#from sqlalchemy.engine import reflection
from sqlalchemy.schema import ForeignKeyConstraint
#from py2neo import neo4j

from configman import DBConnManager

class SrcDBProcessor(object):
    def __init__(self):
        wrkDB = DBConnManager()
        self.insp = reflection.Inspector.from_engine(wrkDB.srcengine)

    def iterTables(self):
        for t in insp.get_table_names():
            meta = MetaData()
            yield TableProcessor(Table(t,meta))

class TableProcessor(object):
    
    def __init__(self,saTableMetadata):
        wrkDB = DBConnManager()
        wrkDB.srcinsp.reflecttable(saTableMetadata,None)
        self.query = select([saTableMetadata])
        self.tablename = saTableMetadata.name
        self.allcols = set([x["name"] for x in wrkDB.srcinsp.get_columns(tname)])
        self.pkeycols = set(wrkDB.srcinsp.get_pk_constraint(tname)["constrained_columns"])
        self.fKeysCols = set([x.columns for x in saTableMetadata.constraints if type(x)==ForeignKeyConstraint])
        self.essCols = self.allcols - self.fKeysCols

    def iterRows(self):
        for r in wrkDB.srcconn.execute(self.query):
            yield r
        