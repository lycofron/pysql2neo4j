'''
Created on 24 Apr 2013

@author: theodojo
'''

import ConfigParser
from sqlalchemy import create_engine, MetaData, select
from sqlalchemy.engine import reflection
#from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy import Table, Column, Integer
from customexceptions import *

from py2neo import neo4j

class SourceDb:
    @classmethod
    def getSqlAlchemyConnectionString(cls):
        ''' Return a string in for of:
        dialect+driver://user:password@host/dbname[?key=value..]
        '''
        return None

class ParseConfig(object):
    '''
    Read settings.ini and provide simple objects with configuration values
    '''
    configFile = "settings.ini"

    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('settings.ini')
        SourceDb.rdbms = config.get("SOURCE_DB","rdbms")
        SourceDb.driver = config.get("SOURCE_DB","driver")
        SourceDb.host = config.get("SOURCE_DB","host")
        SourceDb.schema = config.get("SOURCE_DB","schema")
        SourceDb.user = config.get("SOURCE_DB","user")
        SourceDb.password = config.get("SOURCE_DB","pass")

def getTestedSQLDatabase(dburi,tryWrite=False):
    try:
        engine = create_engine(dburi)
        conn = engine.connect()
        insp = reflection.Inspector.from_engine(dburi)
    except Exception as ex:
        raise DbNotFoundException(ex,"Could not connect to DB %s." % dburi)
    try:
        meta = MetaData()
        sampleTblName=insp.get_table_names()[0]
        sampleTbl = Table(sampleTblName,meta)
        insp.reflecttable(sampleTbl,None)
        s=select([sampleTbl])
        result=conn.execute(s)
        _=result.fetchone()
    except Exception as ex:
        raise  DBUnreadableException(ex,"Could not SELECT on DB %s." % dburi)
    if tryWrite:
        if insp.get_table_names():
            raise WorkflowException("DB %s is not empty." % dburi)

        try:
            md = MetaData()
            testTable = Table('example', md, Column('id', Integer, primary_key=True))
            md.create_all(engine)
        except Exception as ex:
            raise DBInsufficientPrivileges(ex,"Failed to create table in DB %s ." % dburi)
        
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
            raise DBInsufficientPrivileges("Exception while testing trivial operations in DB %s." % dburi)
    return engine, conn, insp

class DBConnManager(object):
    sourcedb="mysql+mysqldb://worlduser:123456@127.0.0.1/worlddb?charset=utf8"
    tempdb="mysql+mysqldb://worlduser:123456@127.0.0.1/mig?charset=utf8"
    graphDbConnectionString = "http://localhost:7474/db/data/"
    
    def __init__(self):
        # Get source database and test it
        self.srcengine, self.srcconn, self.srcinsp = getTestedSQLDatabase(self.sourcedb)        
        # Get a temp db to work with intermediate data
        self.tmpengine, self.tmpconn, self.tmpinsp = getTestedSQLDatabase(self.tempdb)        
        self._test_graphdb()
        
            
    def _test_graphdb(self):
        try:
            graph_db = neo4j.GraphDatabaseService(self.graphDbConnectionString)
        except Exception as ex:
            print "Could not connect to graph database. Error:\n" + ex.message
            raise
        
        try:
            q = neo4j.CypherQuery(graph_db,"start n=node(*) where ID(n)>0 return count(n);")
            r,_ = q.execute()
            if r[0][0]>0:
                print "Graph Database is not empty."
        except Exception as ex:
            print "Could not execute query to graph database. Error:\n" + ex.message
            raise
        
        try:
            test_node = graph_db.create({"data":"whatever"})[0]
            fetched = graph_db.node(test_node.id)
            fetched.delete()
        except Exception as ex:
            print "Could not execute simple operations to graph database. Error:\n" + ex.message
            raise
        
        try:
            batch = neo4j.WriteBatch(graph_db)
        except Exception as ex:
            print "Could not get WriteBatch from graph database. Error:\n" + ex.message
            raise
            

        self.graph_db = graph_db
        self.writeBatch = batch
