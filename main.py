'''
Created on 24 Apr 2013

@author: theodojo
'''

##Yeah, this has nothing to do with the rest of the code
##this is just a code testing playground.

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
    for t in sqlDb.iterTables:
        print "Processing foreign keys of table %s..." % t.tableName
        for fk in t.fKeys:
            graphDb.createRelations(fk)
    print "Terminated"
