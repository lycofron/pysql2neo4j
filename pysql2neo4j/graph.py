import string
from py2neo import Graph, authenticate
from py2neo import Node, Relationship
from customexceptions import DbNotFoundException, DBInsufficientPrivileges
from configman import getGraphDBUri, getGraphDBCredentials, DRY_RUN
from configman import MANY_TO_MANY_AS_RELATION, LOG, PERIODIC_COMMIT_EVERY
from configman import OFFLINE_MODE, _cypher_script_path
from py2neo.packages.httpstream.http import SocketError
from os import devnull
from pysql2neo4j.configman import CYPHER_FILESTREAM

_cypher_stream = devnull
if OFFLINE_MODE and not DRY_RUN:
    _cypher_stream = open(_cypher_script_path, "w")


class GraphProc(object):
    '''Handles communication with Neo4j instance. Also, receive output
    from SqlDbInfo and convert it to Neo4j entities.'''

    #Pattern of cypher statement to insert relations from csv files.
    relStatementPat = """USING PERIODIC COMMIT %s
LOAD CSV WITH HEADERS FROM 'file:%s' AS csvLine
MATCH (src:%s { %s}),(dest:%s { %s})
CREATE (src)-[:%s%s]->(dest)"""

    def __init__(self):
        '''Constructor'''
        graphDbUrl = getGraphDBUri()
        graphDbCredentials = getGraphDBCredentials()
        self.graphDb = getTestedNeo4jDB(graphDbUrl, graphDbCredentials)
        self.periodicCommit = PERIODIC_COMMIT_EVERY if PERIODIC_COMMIT_EVERY \
                                else ""

    def __del__(self):
        _cypher_stream.close()

    def cypher_exec(self, statement):
        '''Wrapper to cypher.execute.'''
        if not DRY_RUN:
            if OFFLINE_MODE:
                CYPHER_FILESTREAM.write(statement)
            else:
                self.graphDb.cypher.execute(statement)

    def importTableCsv(self, tableObj):
        '''Imports a table to Neo4j.'''
        if not (tableObj.isManyToMany() \
           and MANY_TO_MANY_AS_RELATION):
            #Standard table import
            LOG.info("Importing %s..." % tableObj.labelName)
            #Match column names with their respective import expression
            colnames = [x for x in tableObj.importCols.keys()]
            colImpExpr = [col.impFunc("csvLine.%s") % name
                          for name, col in tableObj.importCols.items()]
            cols = ["%s: %s" % x for x in zip(colnames, colImpExpr)]
            colClause = string.join(cols, ',')
            createClause = "CREATE (n:%s { %s})" % (tableObj.labelName,
                                                    colClause)
            for f in tableObj.filesWritten:
                periodicCommitClause = "USING PERIODIC COMMIT %s " \
                                        % self.periodicCommit
                importClause = "LOAD CSV WITH HEADERS " + \
                "FROM 'file:%s' AS csvLine " % f
                cypherQuery = periodicCommitClause + importClause + \
                                createClause
                self.cypher_exec(cypherQuery)
        else:
            #Not necessary to import many-to-many tables. So don't.
            LOG.info("Skipping many-to-many table %s..." % tableObj.labelName)

    def createConstraints(self, tableObj):
        '''Creates unique constraints on Neo4j.'''
        label = tableObj.labelName
        LOG.info("Creating constraint on %s..." % tableObj.labelName)
        for col in tableObj.uniqCols:
            statement = """create constraint on (n:%s)
            assert n.%s is unique""" % (label, col.name)
            LOG.debug(statement)
            self.cypher_exec(statement)

    def createIndexes(self, tableObj):
        '''Creates indexes on Neo4j.'''
        label = tableObj.labelName
        LOG.info("Creating indexes on %s..." % label)
        for col in tableObj.idxCols:
            statement = "create index on :%s(%s)" % (label, col.name)
            LOG.debug(statement)
            self.cypher_exec(statement)

    def createRelations(self, tableObj):
        '''Wrapper, basically. Chooses import process to follow'''
        if MANY_TO_MANY_AS_RELATION and (not tableObj.isManyToMany()):
            #For standard table, import its foreign keys
            for fk in tableObj.fKeys:
                self.createRelationsFk(fk)
        else:
            #For many-to-many table, import it as relationships
            self.manyToManyRelations(tableObj)

    def createRelationsFk(self, fKey):
        '''Create relations on Neo4j, based on an sql foreign key.'''
        fkLabel = fKey.table.labelName
        pkLabel = fKey.refTable.labelName
        #Both nodes will be matched against their own primary keys
        #The pattern is like:
        # MATCH (referencing:{referencing primary key}),
        #       (referenced:{referenced primary key})
        fkColsImportExpr = [(name, col.impFunc("csvLine.%s") %
                             name) for name, col in fKey.table.pkCols.items()]
        fkCols = string.join(["%s: %s" % tup for tup in fkColsImportExpr],
                                    ",")
        #We need to match the name of the field on the primary key table
        #with the name of the field on the foreign key table
        pkColsImportExpr = [(fkColName,
                             pkCol.impFunc("csvLine.%s") % pkName) \
                             for (pkName, pkCol), fkColName in \
                             zip(fKey.consCols.items(), fKey.refCols.keys())]
        pkCols = string.join(["%s: %s" % tup
                                     for tup in pkColsImportExpr], ",")
        relType = fKey.relType
        LOG.info("Foreign key to table %s..." % pkLabel)
        #Emit one statement per file written
        for filename in fKey.table.filesWritten:
            statement = self.relStatementPat % (self.periodicCommit,
                                                filename, pkLabel,
                                                pkCols, fkLabel,
                                                fkCols, relType, "")
            LOG.debug(statement)
            self.cypher_exec(statement)

    def manyToManyRelations(self, tableObj):
        '''Transfers a many-to-many table as relationships in Neo4j'''
        #One can never know what's going to go wrong...
        assert len(tableObj.fKeys) == 2
        #We need to know names of fields on two foreign key tables
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
            self.cypher_exec(statement)


def getTestedNeo4jDB(graphDBurl, graphDbCredentials):
    '''Gets a Neo4j url and returns a GraphDatabaseService to the database
    after having performed some trivial tests'''
    try:
        if graphDbCredentials:
            authenticate(*graphDbCredentials)
        graphDb = Graph(graphDBurl)
        #just fetch a Node to check we are connected
        #even in DRY RUN we should check Neo4j connectivity
        #but not in OFFLINE MODE
        if not OFFLINE_MODE:
            _ = iter(graphDb.match(limit=1)).next()
    except StopIteration:
        pass
    except SocketError as ex:
        raise DbNotFoundException(ex, "Could not connect to Graph DB %s."
                                  % graphDBurl)

    if not DRY_RUN and not OFFLINE_MODE:
        try:
            test_node = Node("TEST", data="whatever")
            graphDb.create(test_node)
            graphDb.delete(test_node)
        except Exception as ex:
            raise DBInsufficientPrivileges(\
                    "Failed on trivial operations in DB %s." % graphDBurl)

    return graphDb


def createModelGraph(sqlDb, graphDb):
    tableNodes = dict()
    for t in sqlDb.tableList:
        r = t.asNodeInfo()
        if r:
            labels, properties = r
            tableNodes[t.labelName] = Node(*labels, **properties)
    if not DRY_RUN:
        if OFFLINE_MODE:
            pass
        else:
            graphDb.graphDb.create(*tableNodes.values())
    relations = list()
    for t in sqlDb.tableList:
        r = t.asRelInfo()
        if r:
            src, relType, dest, properties = r
            relations.append(Relationship(tableNodes[src], relType,
                                          tableNodes[dest], **properties))
        for fk in t.fKeys:
            r = fk.asRelInfo()
            if r:
                src, relType, dest, properties = r
            relations.append(Relationship(tableNodes[src], relType,
                                          tableNodes[dest], **properties))
    if not DRY_RUN:
        graphDb.graphDb.create(*relations)
