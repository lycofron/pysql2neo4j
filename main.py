'''
Created on 24 Apr 2013

@author: theodojo
'''

##Yeah, this has nothing to do with the rest of the code, this is just a code testing playground.

from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.engine import reflection
from py2neo import node, rel
from py2neo import neo4j
from pysql2neo4j.pysql2neo4j import SrcDBProcessor

sourcedb="mysql+mysqlconnector://worlduser:123456@127.0.0.1/world?charset=utf8"
graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

if __name__ == '__main__':
#    batch = neo4j.WriteBatch(graph_db)
    cmdCount=0
    cmdCountCycle=1000
    cmdCountTotal=0
    
    #Phase 1: Create Nodes
    for t in SrcDBProcessor().iterTables():
        print t.tablename
        cmdCount=cmdCount+1
        for row in t.iterRows():
            mainNodeData={}
            for c in t.esscols: mainNodeData[c]=row[c]
#            n=batch.create(mainNodeData)
#            batch.add_labels(n,t)
            cmdCount=cmdCount+1
            if cmdCount > cmdCountCycle:
#                batch.submit()
                cmdCountTotal = cmdCountTotal + cmdCount
                cmdCount = 0
                print "Sent %d commands." % cmdCountTotal
        if cmdCount>0:
#            batch.submit()
            print "Sent %d leftover commands." % cmdCount
    print "Total %d commands sent." % cmdCountTotal
    print "Terminated"