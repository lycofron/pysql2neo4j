import string
from py2neo import Graph, authenticate
from py2neo import Node
from customexceptions import DBUnreadableException, DBInsufficientPrivileges
from configman import getGraphDBUri, getGraphDBCredentials, confDict


class GraphProc(object):
    relStatementPat = """USING PERIODIC COMMIT
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

#     def graphSearch(self, label, properties=None, limit=None, skip=None, orderBy=None):
#         """ Modified from py2neo.core.Graph.find method: find all nodes matching given criteria.
#             Search on multiple properties allowed.
#             Added skip, order by clauses
#             Return iterator over results
#         """
#         if not label:
#             raise ValueError("Empty label")
#         from py2neo.cypher.lang import cypher_escape
#         conditions = None
#         parameters = {}
#         if properties:
#             conditionals = list()
#             for idx, k in enumerate(properties.keys()):
#                 paramSymbol = "V%d" % idx
#                 conditionals.append("%s:{%s}" % (cypher_escape(k), paramSymbol))
#                 parameters[paramSymbol] = properties[k]
#             conditions = string.join(conditionals, ",")
#         if conditions is None:
#             statement = "MATCH (n:%s) RETURN n,labels(n)" % cypher_escape(label)
#         else:
#             statement = "MATCH (n:%s {%s}) RETURN n,labels(n)" % (
#                 cypher_escape(label), conditions)
#         if orderBy:
#             statement += " ORDER BY %s" % string.join(["n.%s" % cypher_escape(x)
#                                                        for x in orderBy],
#                                                       ",")
#         if skip:
#             statement += " SKIP %s" % skip
#         if limit:
#             statement += " LIMIT %s" % limit
#         response = self.cypher.post(statement, parameters)
#         for record in response.content["data"]:
#             dehydrated = record[0]
#             dehydrated.setdefault("metadata", {})["labels"] = record[1]
#             yield self.hydrate(dehydrated)
#         response.close()

    def importTableCsv(self, tableObj):
        print "Importing %s..." % tableObj.labelName
        cols = ["%s: line.%s" % (col, col) for col in tableObj.allcols]
        colClause = string.join(cols, ',')
        createClause = "CREATE (n:%s { %s})" % (tableObj.labelName, colClause)
        for f in tableObj.filesWritten:
            importClause = "LOAD CSV WITH HEADERS FROM 'file:%s' AS line " % f
            cypherQuery = importClause + createClause
            self.graphDb.cypher.run(cypherQuery)

    def createIndexes(self, tableObj):
        if len(tableObj.pkeycols) == 1:
            print "Creating constraint on %s..." % tableObj.labelName
            statement = """create constraint on (n:%s)
            assert n.%s is unique""" % (tableObj.labelName,
                                        tableObj.pkeycols[0])
            self.graphDb.cypher.run(statement)
        else:
            print "Creating indexes on %s..." % tableObj.labelName
            for col in tableObj.pkeycols:
                statement = "create index on :%s(%s)" % (tableObj.labelName,
                                                         col)
                self.graphDb.cypher.run(statement)

    def createRelations(self, foreignKey):
        sourceLabel = foreignKey.table.labelName
        targetLabel = foreignKey.refTable.labelName
        keyColsSource = string.join(["%s: csvLine.%s" % (col, col)
                                     for col in foreignKey.table.pkeycols],
                                    ",")
        keyColsTarget = string.join(["%s: csvLine.%s" %
                                     (col["referenced"], col["referencing"])
                                     for col in foreignKey.cols],
                                     ",")
        relType = self.__transformRelTypes(sourceLabel + '_RL_' + targetLabel)
        print "Foreign key to table %s..." % targetLabel
        for filename in foreignKey.table.filesWritten:
            statement = self.relStatementPat % (filename, sourceLabel,
                                                keyColsSource, targetLabel,
                                                keyColsTarget, relType)
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
