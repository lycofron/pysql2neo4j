import string
from py2neo import Graph, authenticate
from py2neo import Node
from customexceptions import DbNotFoundException, DBInsufficientPrivileges
from configman import getGraphDBUri, getGraphDBCredentials, DRY_RUN
from configman import MANY_TO_MANY_AS_RELATION, LOG, PERIODIC_COMMIT_EVERY


class GraphProc(object):
    relStatementPat = """USING PERIODIC COMMIT %d
LOAD CSV WITH HEADERS FROM 'file:%s' AS csvLine
MATCH (src:%s { %s}),(dest:%s { %s})
CREATE (src)-[:%s%s]->(dest)"""

    def __init__(self):
        graphDbUrl = getGraphDBUri()
        graphDbCredentials = getGraphDBCredentials()
        self.graphDb = getTestedNeo4jDB(graphDbUrl, graphDbCredentials)
        self.periodicCommit = PERIODIC_COMMIT_EVERY

    def importTableCsv(self, tableObj):
        if not (tableObj.isManyToMany() \
           and MANY_TO_MANY_AS_RELATION):
            LOG.info("Importing %s..." % tableObj.labelName)
            colnames = [x for x in tableObj.importCols.keys()]
            colImpExpr = [col.impFunc("csvLine.%s") % name
                          for name, col in tableObj.importCols.items()]
            cols = ["%s: %s" % x for x in zip(colnames, colImpExpr)]
            colClause = string.join(cols, ',')
            createClause = "CREATE (n:%s { %s})" % (tableObj.labelName,
                                                    colClause)
            for f in tableObj.filesWritten:
                periodicCommitClause = "USING PERIODIC COMMIT %d " % \
                                    self.periodicCommit
                importClause = "LOAD CSV WITH HEADERS " + \
                "FROM 'file:%s' AS csvLine " % f
                cypherQuery = periodicCommitClause + importClause + \
                                createClause
                if not DRY_RUN:
                    self.graphDb.cypher.run(cypherQuery)
        else:
            LOG.info("Skipping many-to-many table %s..." % tableObj.labelName)

    def createConstraints(self, tableObj):
        label = tableObj.labelName
        LOG.info("Creating constraint on %s..." % tableObj.labelName)
        for col in tableObj.uniqCols:
            statement = """create constraint on (n:%s)
            assert n.%s is unique""" % (label, col.name)
            LOG.debug(statement)
            if not DRY_RUN:
                self.graphDb.cypher.run(statement)

    def createIndexes(self, tableObj):
        label = tableObj.labelName
        LOG.info("Creating indexes on %s..." % label)
        for col in tableObj.idxCols:
            statement = "create index on :%s(%s)" % (label, col.name)
            LOG.debug(statement)
            if not DRY_RUN:
                self.graphDb.cypher.run(statement)

    def createRelations(self, tableObj):
        if MANY_TO_MANY_AS_RELATION and (not tableObj.isManyToMany()):
            for fk in tableObj.fKeys:
                self.createRelationsFk(fk)
        else:
            self.manyToManyRelations(tableObj)

    def createRelationsFk(self, fKey):
        fkLabel = fKey.table.labelName
        pkLabel = fKey.refTable.labelName
        fkColsImportExpr = [(name, col.impFunc("csvLine.%s") %
                             name) for name, col in fKey.table.pkCols.items()]
        fkCols = string.join(["%s: %s" % tup for tup in fkColsImportExpr],
                                    ",")
        pkColsImportExpr = [(fkColName,
                             pkCol.impFunc("csvLine.%s") % pkName) \
                             for (pkName, pkCol), fkColName in \
                             zip(fKey.consCols.items(), fKey.refCols.keys())]
        pkCols = string.join(["%s: %s" % tup
                                     for tup in pkColsImportExpr], ",")
        relType = fKey.relType
        LOG.info("Foreign key to table %s..." % pkLabel)
        for filename in fKey.table.filesWritten:
            statement = self.relStatementPat % (self.periodicCommit,
                                                filename, pkLabel,
                                                pkCols, fkLabel,
                                                fkCols, relType, "")
            LOG.debug(statement)
            if not DRY_RUN:
                self.graphDb.cypher.run(statement)

    def manyToManyRelations(self, tableObj):
        assert len(tableObj.fKeys) == 2
        src = tableObj.fKeys[0]
        dest = tableObj.fKeys[1]
        pk1Label = src.refTable.labelName
        pk2Label = dest.refTable.labelName
        pk1ColsImportExpr = [(refColName, pkCol.impFunc("csvLine.%s") %
                             pkName) for (pkName, pkCol), refColName in \
                             zip(src.consCols.items(), src.refCols.keys())]
        pk1Cols = string.join(["%s: %s" % tup for tup in pk1ColsImportExpr],
                                    ",")
        pk2ColsImportExpr = [(refColName, pkCol.impFunc("csvLine.%s") %
                              pkName) for (pkName, pkCol), refColName in \
                             zip(dest.consCols.items(), dest.refCols.keys())]
        pk2Cols = string.join(["%s: %s" % tup
                                     for tup in pk2ColsImportExpr], ",")
        assert hasattr(tableObj, 'relType')
        relType = tableObj.relType
        LOG.info("Importing many-to-many table %s as relationships..." %
                 tableObj.tableName)
        colnames = [x for x in tableObj.importCols.keys()]
        colImpExpr = [col.impFunc("csvLine.%s") % name
                      for name, col in tableObj.importCols.items()]
        cols = ["%s: %s" % x for x in zip(colnames, colImpExpr)]
        colClause = "{%s}" % string.join(cols, ',') if cols else ""
        for filename in tableObj.filesWritten:
            statement = self.relStatementPat % (self.periodicCommit,
                                                filename, pk2Label,
                                                pk2Cols, pk1Label,
                                                pk1Cols, relType, colClause)
            LOG.debug(statement)
            if not DRY_RUN:
                self.graphDb.cypher.run(statement)


def getTestedNeo4jDB(graphDBurl, graphDbCredentials):
    '''Gets a Neo4j url and returns a GraphDatabaseService to the database
    after having performed some trivial tests'''
    try:
        if graphDbCredentials:
            authenticate(*graphDbCredentials)
        graphDb = Graph(graphDBurl)
    except Exception as ex:
        raise DbNotFoundException(ex, "Could not connect to Graph DB %s."
                                  % graphDBurl)

    if not DRY_RUN:
        try:
            test_node = Node("TEST", data="whatever")
            graphDb.create(test_node)
            graphDb.delete(test_node)
        except Exception as ex:
            raise DBInsufficientPrivileges(\
                    "Failed on trivial operations in DB %s." % graphDBurl)

    return graphDb
