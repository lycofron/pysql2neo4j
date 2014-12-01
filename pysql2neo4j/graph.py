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
        if confDict['transformRelTypes'] == 'allcaps':
            self.__transformRelTypes = string.upper
        else:
            self.__transformRelTypes = lambda x: x
        self.graphDb = getTestedNeo4jDB(graphDbUrl, graphDbCredentials)
        self.periodicCommit = confDict["periodiccommitevery"]

    def importTableCsv(self, tableObj):
        print "Importing %s..." % tableObj.labelName
        colnames = [col.name for col in tableObj.cols]
        colImpExpr = [col.handler.impFunc("csvLine.%s") % col.name
                      for col in tableObj.cols]
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
        if len(tableObj.pkeycols) == 1:
            print "Creating constraint on %s..." % tableObj.labelName
            statement = """create constraint on (n:%s)
            assert n.%s is unique""" % (tableObj.labelName,
                                        tableObj.pkeycols[0].name)
            print statement
            self.graphDb.cypher.run(statement)
        else:
            print "Creating indexes on %s..." % tableObj.labelName
            for col in tableObj.pkeycols:
                statement = "create index on :%s(%s)" % (tableObj.labelName,
                                                         col.name)
                print statement
                self.graphDb.cypher.run(statement)

    def createRelations(self, fKey):
        fkLabel = fKey.table.labelName
        pkLabel = fKey.refTable.labelName
        fkColsImportExpr = [(col.name, col.handler.impFunc("csvLine.%s") %
                             col.name) for col in fKey.table.pkeycols]
        fkCols = string.join(["%s: %s" % tup for tup in fkColsImportExpr],
                                    ",")
        pkColsImportExpr = [(col[1].name,
                             col[0].handler.impFunc("csvLine.%s") % \
                             col[0].name) for col in \
                             zip(fKey.consCols, fKey.refCols)]
        pkCols = string.join(["%s: %s" % tup
                                     for tup in pkColsImportExpr], ",")
        relType = self.__transformRelTypes(fkLabel + '_RL_' + pkLabel)
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
