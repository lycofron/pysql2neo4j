import string
from py2neo import Graph


class GraphProc(object):

    def __init__(self, graphUri):
        self.graphDb = Graph(graphUri)

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
        print "Importing %s..." % tableObj.tablename
        cols = ["%s: line.%s" % (col, col) for col in tableObj.allcols]
        colClause = string.join(cols, ',')
        createClause = "CREATE (n:%s { %s})" % (tableObj.tablename, colClause)
        for f in tableObj.filesWritten:
            importClause = "LOAD CSV WITH HEADERS FROM 'file:%s' AS line " % f
            cypherQuery = importClause + createClause
            self.graphDb.cypher.run(cypherQuery)

    def createIndexes(self, tableObj):
        if len(tableObj.pkeycols) == 1:
            print "Creating constraint on %s..." % tableObj.tablename
            statement = """create constraint on (n:%s)
            assert n.%s is unique""" % (tableObj.tablename,
                                        tableObj.pkeycols[0])
            self.graphDb.cypher.run(statement)
        else:
            print "Creating indexes on %s..." % tableObj.tablename
            for col in tableObj.pkeycols:
                statement = "create index on :%s(%s)" % (tableObj.tablename,
                                                         col)
                self.graphDb.cypher.run(statement)
