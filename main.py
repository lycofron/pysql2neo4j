'''
Created on 24 Apr 2013

@author: theodojo
'''

from pysql2neo4j.rdbmsproc import SqlDbInfo
from pysql2neo4j.graph import GraphProc, createModelGraph
from pysql2neo4j.configman import LOG, DRY_RUN, OFFLINE_MODE

if __name__ == '__main__':
    try:
        #Step 0: Initialize
        LOG.info("Initializing...")
        if OFFLINE_MODE:
            LOG.info("Running in OFFLINE mode (producing files to import).")
        if DRY_RUN:
            LOG.info("Performing DRY RUN (no changes/files will be written).")
        sqlDb = SqlDbInfo()
        graphDb = GraphProc()

        #Step 1: Export tables as csv files
        sqlDb.export()

        LOG.info("\nFinished export.\n\nStarting import...")

        #Step 2: Import Nodes
        for t in sqlDb.tableList:
            graphDb.importTableCsv(t)

        #Step 3: Create constraints and indexes
        for t in sqlDb.tableList:
            graphDb.createConstraints(t)
            graphDb.createIndexes(t)

        LOG.info("\nFinished import.\n\nAdding relations...")

        #Step 4: Create relations
        for t in sqlDb.tableList:
            LOG.info("Processing foreign keys of table %s..." % t.tableName)
            graphDb.createRelations(t)

        #Step 5: Courtesy representation of graph model :)
        createModelGraph(sqlDb, graphDb)

        LOG.info("Terminated")
    except:
        LOG.exception("Unexpected condition!")
        LOG.critical("Terminated abnormally")
    finally:
        graphDb.__del__()
