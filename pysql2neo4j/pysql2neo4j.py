'''
Created on 04 May 2013

@author: theodojo
'''

from sqlalchemy import select #,create_engine, MetaData, Table 
#from sqlalchemy.engine import reflection
from sqlalchemy.schema import ForeignKeyConstraint
#from py2neo import neo4j

from configman import DBConnManager

class TableProcessor(object):
    
    def __init__(self,saTableMetadata):
        LocEnv = DBConnManager()
        LocEnv.srcinsp.reflecttable(saTableMetadata,None)
        s = select([saTableMetadata])
        tname = saTableMetadata.name
        self._result = LocEnv.srcconn.execute(s)
        cols = set([x["name"] for x in LocEnv.srcinsp.get_columns(tname)])
        self.pkeycols = set(LocEnv.srcinsp.get_pk_constraint(tname)["constrained_columns"])
        self.fKeysCols = [x.columns for x in saTableMetadata.constraints if type(x)==ForeignKeyConstraint]
        fkeycols = set()
        for x in self.fKeysCols:
            fkeycols = fkeycols | set(x)
        self.essCols = cols - fkeycols

        