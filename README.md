pysql2neo4j
===========

This is another take on the idea of a generic tool to migrate an SQL database to the Neo4j graph database, this time taking advantage of sqlalchemy.

* WORK IN PROGRESS - DOES NOT WORK YET *

Description
-----------

The basics:

 - Connect to the SQL database
 - Inspect its schema using sqlalchemy
 - Generate neo4j nodes
 - Insert them to neo4j db.

Considerations:

 - Maybe we mustn't alter the source database at all. For this purpose, we put aside a temporary db to store relations-related :-)data.
 - The only way to get information about relations in source database, is from foreign keys. So, you'd better already have them in it, or this tool won't help.

More to come (hopefully)...
