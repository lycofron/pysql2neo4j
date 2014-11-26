import string
from py2neo import Graph

class GraphExt(Graph):
    def graphSearch(self, label, properties=None, limit=None, skip=None, orderBy=None):
        """ Modified from py2neo.core.Graph.find method: find all nodes matching given criteria. 
            Search on multiple properties allowed.
            Added skip, order by clausess
            Return iterator over results
        """
        if not label:
            raise ValueError("Empty label")
        from py2neo.cypher.lang import cypher_escape
        conditions= None
        parameters = {}
        if properties:
            conditionals=list()
            for idx,k in enumerate(properties.keys()):
                paramSymbol="V%d" % idx
                conditionals.append("%s:{%s}" % (cypher_escape(k),paramSymbol))
                parameters[paramSymbol]=properties[k]
            conditions = string.join(conditionals,",")
        if conditions is None:
            statement = "MATCH (n:%s) RETURN n,labels(n)" % cypher_escape(label)
        else:
            statement = "MATCH (n:%s {%s}) RETURN n,labels(n)" % (
                cypher_escape(label), conditions)
        if orderBy:
            statement += " ORDER BY %s" % string.join(["n.%s" % cypher_escape(x) for x in orderBy],",")
        if skip:
            statement += " SKIP %s" % skip
        if limit:
            statement += " LIMIT %s" % limit
        response = self.cypher.post(statement, parameters)
        for record in response.content["data"]:
            dehydrated = record[0]
            dehydrated.setdefault("metadata", {})["labels"] = record[1]
            yield self.hydrate(dehydrated)
        response.close()
