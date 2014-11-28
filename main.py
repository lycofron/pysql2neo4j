'''
Created on 24 Apr 2013

@author: theodojo
'''

##Yeah, this has nothing to do with the rest of the code
##this is just a code testing playground.

import string
from pysql2neo4j.rdbmsproc import SqlDbInfo
from pysql2neo4j.graph import GraphProc

### Detailed output
# from py2neo import watch
# watch("httpstream")


if __name__ == '__main__':
    print "Initializing..."
    sqlDb = SqlDbInfo()
    graphDb = GraphProc()

    #Step 1: Export tables as csv files
    sqlDb.export()

    print "\nFinished export.\n\nStarting import..."

    #Step 2: Import Nodes
    for t in sqlDb.iterTables:
        graphDb.importTableCsv(t)

    #Step 3: Create constraints or indexes
    for t in sqlDb.iterTables:
        graphDb.createIndexes(t)

    print "\nFinished import.\n\nAdding relations..."

    #Phase 4: Create relations
    statementPattern = """USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 'file:%s' AS csvLine
MATCH (src:%s { %s}),(dest:%s { %s})
CREATE (src)-[:%s]->(dest)"""
    for t in sqlDb.iterTables:
        print "Processing foreign keys of table %s..." % t.tablename
        for fk in t.foreignKeys:
            sourceLabel = fk.table.tablename
            targetLabel = fk.refTable.tablename
            keyColsSource = string.join(["%s: csvLine.%s" % (col, col) for col in fk.table.pkeycols], ",")
            keyColsTarget = string.join(["%s: csvLine.%s" % (col["referenced"], col["referencing"]) for col in fk.cols], ",")
            relType = sourceLabel + '_RL_' + targetLabel
            print "Foreign key to table %s..." % targetLabel
            for filename in fk.table.filesWritten:
                statement = statementPattern % (filename, sourceLabel, keyColsSource, targetLabel, keyColsTarget, relType)
                graphDb.graphDb.cypher.run(statement)

    print "Terminated"
