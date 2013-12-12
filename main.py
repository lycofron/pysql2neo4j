'''
Created on 24 Apr 2013

@author: theodojo
'''

from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.engine import reflection
from py2neo import node, rel
from py2neo import neo4j


sourcedb="mysql+mysqldb://worlduser:123456@127.0.0.1/worlddb?charset=utf8"
graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

if __name__ == '__main__':
    engine = create_engine(sourcedb)
    conn = engine.connect()
    insp = reflection.Inspector.from_engine(engine)
    meta = MetaData()
    batch = neo4j.WriteBatch(graph_db)
    cmdCount=0
    cmdCountCycle=1000
    cmdCountTotal=0
    
    #Phase 1: Create Nodes
    for t in insp.get_table_names():
        print t
        cmdCount=cmdCount+1
        cols = set([x["name"] for x in insp.get_columns(t)])
        pkeycols = set(insp.get_pk_constraint(t)["constrained_columns"])
        curTable = Table(t,meta)
        insp.reflecttable(curTable,None)
        s = select([curTable])
        result = conn.execute(s)
        for row in result:
            mainNodeData={"table":t}
            for c in cols: mainNodeData[c]=row[c]
            batch.create(mainNodeData)
            cmdCount=cmdCount+1
            if cmdCount > cmdCountCycle:
                batch.submit()
                cmdCountTotal = cmdCountTotal + cmdCount
                cmdCount = 0
                print "Sent %d commands." % cmdCountTotal
        if cmdCount>0:
            batch.submit()
