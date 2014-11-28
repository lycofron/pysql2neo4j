# pysql2neo4j

(Aiming to be) a script to automatically migrate relational databases to Neo4J.

It is based on SqlAlchemy and py2neo.

### What it does (in brief):

-sqlalchemy inspects the database
-tables are extracted to CSV files
-csv files are imported into Neo4j
-Constraints or indexes are created for each primary key
-relations are imported

So far, the script has succesfully migrated SAKILA sample database on MySQL. No other tests have been performed, though.

### USAGE

You need to edit file settings.ini before running main.py. Parameter documentation is provided in the file.

### LAST BUT NOT LEAST
---
This script is still in a very early, experimental change. Use at own risk.

