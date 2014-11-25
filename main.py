'''
Created on 24 Apr 2013

@author: theodojo
'''

##Yeah, this has nothing to do with the rest of the code, this is just a code testing playground.
import sys
import string
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.engine import reflection
from py2neo import Node, Relationship
from py2neo import Graph
from py2neo.cypher import cypher_escape
from pysql2neo4j.pysql2neo4j import SqlDbProcessor

#sourcedb="mysql+mysqlconnector://sakilauser:123456@127.0.0.1/sakila?charset=utf8"
graph_db = Graph("http://localhost:7474/db/data/")

if __name__ == '__main__':
    cmdCount=0
    cmdCountCycle=150
    cmdCountTotal=0
    nodeList=[]
    
    #Phase 1: Create Nodes with their primary keys
    for t in SqlDbProcessor().iterTables:
        print t.tablename
        tblLabel = t.tablename
        pkLabel = t.tablename + "_PK"
        cmdCount=cmdCount+1
        for row in t.iterRows():
            mainNodeData={}
            for c in t.allcols: mainNodeData[c]=row[c]
            n=Node(tblLabel,**mainNodeData)
            nodeList.append(n)
               
            cmdCount=cmdCount+1
               
            if cmdCount >= cmdCountCycle:
                try:
                    graph_db.create(*nodeList)
                    nodeList=[]
                except Exception as ex:
                    print nodeList
                    print ex.message
                    sys.exit(-1)
                cmdCountTotal = cmdCountTotal + cmdCount
                cmdCount = 0
                print "Sent %d commands." % cmdCountTotal
    if cmdCount>0:
        graph_db.create(*nodeList)
        nodeList=[]
        cmdCountTotal = cmdCountTotal + cmdCount
    print "Total %d commands sent." % cmdCountTotal
    
    
    #Phase 2: Relate nodes
    relList = []
    cmdCount=0
    for t in SqlDbProcessor().iterTables:
        if t.foreignKeys:
            print "Processing %s." % t.tablename
            for node in graph_db.find(label=t.tablename):
                for fk in t.foreignKeys:
                    conditions = string.join(["%s:%s" % (col, node[col]) for col in fk.cols if node[col]],",")
                    if not conditions: continue
                    cypher_query = "match (n:%s{%s}) return n;" % (fk.refTable.tablename,conditions)
                    records = graph_db.cypher.execute(cypher_query)
                    subgraph = records.to_subgraph()
                    assert len(subgraph.nodes)==1
                    targetNode = next(iter(subgraph.nodes))
                    relList.append(Relationship(node,t.tablename + "_RL_" + fk.refTable.tablename,targetNode))
                    cmdCount=cmdCount+1
                      
                    if cmdCount >= cmdCountCycle:
                        try:
                            graph_db.create(*relList)
                            relList=[]
                        except Exception as ex:
                            print relList
                            print ex.message
                            sys.exit(-1)
                        cmdCountTotal = cmdCountTotal + cmdCount
                        cmdCount = 0
                        print "Sent %d commands." % cmdCountTotal
        else:
            print "Skipping %s..." % t.tablename
    if cmdCount>0:
        graph_db.create(*nodeList)
        nodeList=[]
        cmdCountTotal = cmdCountTotal + cmdCount

    print "Total %d commands sent." % cmdCountTotal
    print "Terminated"