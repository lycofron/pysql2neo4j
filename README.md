pysql2neo4j
-----------

(Aiming to be) a script to automatically migrate relational databases to Neo4J.

It is based on SqlAlchemy and py2neo.

SqlAlchemy serves not only as an interface to many RDBMS's, but also makes it possible to inspect the database.

What it does (in brief):
-Gets table names
-Gets information about primary and foreign keys
-Creates one node (on Neo4J) for each row on the RDBMS. The node's label is the name of the table the original row belongs to.
-When it finds foreign keys, it creates relationships. The relationship type will be "REFERENCINGTABLE\_RL\_REFERENCEDTABLE) and the direction of the relationship will be from referencing table to referenced.

So far, the script has succesfully migrated SAKILA sample database on MySQL. No other tests have been performed, though.

USAGE
---
Do NOT edit settings.ini (this will be functional in the future). Instead, just edit lines 121-122 on configman.py.
- sourcedb must be an SqlAlchemy connection string.
- graphDbConnectionString must be the base URI of the graph database

Then run main.py

---

IMPORTANT NOTE
---
This script is still in a very early, experimental change. Use at own risk.

