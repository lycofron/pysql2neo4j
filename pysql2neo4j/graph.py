import string
from py2neo import Graph, authenticate
from py2neo import Node
from customexceptions import DBUnreadableException, DBInsufficientPrivileges
from configman import getGraphDBUri, getGraphDBCredentials, confDict


class GraphProc(object):
    relStatementPat = """USING PERIODIC COMMIT %d
LOAD CSV WITH HEADERS FROM 'file:%s' AS csvLine
MATCH (src:%s { %s}),(dest:%s { %s})
CREATE (src)-[:%s]->(dest)"""

    def __init__(self):
        graphDbUrl = getGraphDBUri()
        graphDbCredentials = getGraphDBCredentials()
        self.graphDb = getTestedNeo4jDB(graphDbUrl, graphDbCredentials)
        self.periodicCommit = confDict["periodiccommitevery"]

    def importTableCsv(self, tableObj):
        print "Importing %s..." % tableObj.labelName
        colnames = [x for x in tableObj.cols.keys()]
        colImpExpr = [col.impFunc("csvLine.%s") % name
                      for name, col in tableObj.cols.items()]
        cols = ["%s: %s" % x for x in zip(colnames, colImpExpr)]
        colClause = string.join(cols, ',')
        createClause = "CREATE (n:%s { %s})" % (tableObj.labelName, colClause)
        for f in tableObj.filesWritten:
            periodicCommitClause = "USING PERIODIC COMMIT %d " %\
                                self.periodicCommit
            importClause = "LOAD CSV WITH HEADERS FROM 'file:%s' AS csvLine "\
                            % f
            cypherQuery = periodicCommitClause + importClause + createClause
            self.graphDb.cypher.run(cypherQuery)

    def createIndexes(self, tableObj):
        label = tableObj.labelName
        if tableObj.hasCompositePK():
            print "Creating indexes on %s..." % label
            for col in tableObj.pkCols.keys():
                statement = "create index on :%s(%s)" % (label, col)
                print statement
                self.graphDb.cypher.run(statement)
        else:
            print "Creating constraint on %s..." % tableObj.labelName
            field = iter(tableObj.pkCols.keys()).next()
            statement = """create constraint on (n:%s)
            assert n.%s is unique""" % (label, field)
            print statement
            self.graphDb.cypher.run(statement)

    def createRelations(self, fKey):
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
        print "Foreign key to table %s..." % pkLabel
        for filename in fKey.table.filesWritten:
            statement = self.relStatementPat % (self.periodicCommit,
                                                filename, pkLabel,
                                                pkCols, fkLabel,
                                                fkCols, relType)
            print statement
            self.graphDb.cypher.run(statement)


def getTestedNeo4jDB(graphDBurl, graphDbCredentials):
    '''Gets a Neo4j url and returns a GraphDatabaseService to the database
    after having performed some trivial tests'''
    try:
        if graphDbCredentials:
            authenticate(*graphDbCredentials)
        graphDb = Graph(graphDBurl)
    except Exception as ex:
        raise DBUnreadableException(ex, "Could not connect to graphDb database.")

    try:
        test_node = Node("TEST", data="whatever")
        graphDb.create(test_node)
        graphDb.delete(test_node)
    except Exception as ex:
        raise DBInsufficientPrivileges(ex,
            "Could not execute simple operations to graphDb database.")

    return graphDb
