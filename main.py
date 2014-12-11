'''
Created on 24 Apr 2013

@author: theodojo
'''

from pysql2neo4j.rdbmsproc import SqlDbInfo
from pysql2neo4j.graph import GraphProc, createModelGraph
from pysql2neo4j import configman

conf = configman.conf

if __name__ == '__main__':
    try:
        #Step 0: Initialize
        conf.LOG.info("Initializing...")
        if conf.OFFLINE_MODE:
            conf.LOG.info("Running in OFFLINE mode (producing files to import).")
        if conf.DRY_RUN:
            conf.LOG.info("Performing DRY RUN (no changes/files will be written).")
        sqlDb = SqlDbInfo()
        graphDb = GraphProc()

        #Step 1: Export tables as csv files
        sqlDb.export()

        conf.LOG.info("\nFinished export.\n\nStarting import...")

        #Step 2: Import Nodes
        for t in sqlDb.tableList:
            graphDb.importTableCsv(t)

        #Step 3: Create constraints and indexes
        for t in sqlDb.tableList:
            graphDb.createConstraints(t)
            graphDb.createIndexes(t)

        conf.LOG.info("\nFinished import.\n\nAdding relations...")

        #Step 4: Create relations
        for t in sqlDb.tableList:
            conf.LOG.info("Processing foreign keys of table %s..." %
                          t.tableName)
            graphDb.createRelations(t)

        #Step 5: Courtesy representation of graph model :)
        createModelGraph(sqlDb, graphDb)

        conf.LOG.info("Terminated")
    except:
        conf.LOG.exception("Unexpected condition!")
        conf.LOG.critical("Terminated abnormally")
    finally:
        graphDb.__del__()
