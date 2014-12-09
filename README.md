# pysql2neo4j

A script to automatically migrate relational databases to Neo4J.

It is based on SqlAlchemy and py2neo.

### Motivation / Intended audience

When I first discovered Neo4J, I thought it was great. I wanted to try it but I needed a database I was familiar with to try it out. I had a couple of relational databases at the time, but I did not like the idea of loading each table separately, then setting up relationships.

I thought there should be a tool to do the job automatically.

So, here it is. This script is meant to extract an SQL database and migrate its data to Neo4j automatically, with some simple modifications on the model. It is intended to be used by people who need some data on Neo4j to play with and get them quickly up and running, or to provide a starting point from which they can build their graph model. It is not meant to provide an optimal transformation of relational data as a graph - after all, "optimal", in this case, is a matter of personal taste.

### Requirements

 - Python 2.7
   - SQLAlchemy 0.8.3 or newer
   - py2neo 2.0
   - unicodecsv 0.9.4
   - you need to have an SQLAlchemy-supported DBAPI. In development, mysql-connector v.1.0.12 was used.
 - Neo4j installed locally. Version 2.1.5 was used during development.
 - The database to be migrated
   - must declare a primary key on every table
   - must have all foreign keys declared

(Well, I understand that the aforementioned requirements leave out approximately 95% of Real World databases :) )

### What it does (in brief):

 * sqlalchemy inspects the database and gets schema information
 * tables are extracted to CSV files
 * csv files are imported into Neo4j
 * Constraints or indexes are created for each primary key
 * relations are imported
 * the model of imported data is created as a graph, for reference

### What it does (in detail):

Via sqlalchemy, the script gets all necessary information about the relational database's structure: tables, primary keys, foreign keys and columns with their data types.

Using this information, it extracts each table to a bunch of CSV files, encoded in utf-8. Each file contains, by default, 10K rows - but you can change this if you want.

For each CSV file that was exported, a corresponding Cypher command is issued to Neo4j in order to load it to Neo4j, using periodic commit (set to 500 for testing, but also configurable). Each row is imported as a node labeled after the table name, by default Capitalized (i.e. employees => Employees).

Then constraints and indexes are created. Single-field primary keys and unique indexes are directly converted to unique constraints in Neo4j. Composite primary keys and unique indexes, are converted to multiple single-field indexes.

In the last step, using the same CSV files, relations are created. The relationship type is all caps, in the form of REFERENCED_REFERENCING and with direction from referenced to referencing.

In addition to the above, the script will create a graph representation of the structure of imported data. The nodes will have two labels (*Pysql2neo4j* and *SchemaInfo*) with table name as property *\_\_tablename*. They will be connected by the relationships that are connecting the corresponding nodes. Relationships will have their name as *property \_\_relationType*. The other properties will be in the form *sourceTable.sourceField : destinationTable.destinationField*.

### Transformations in schema

In the process, the script does the following changes (all of them being default, but optional):

 - It removes redundant fields i.e. fields that (1) are part of a foreign key and (2) do not participate in a primary key
 - Tables implementing a many-to-many relationship are not imported as nodes but as relationships. Non-redundant fields of these tables become relationship properties.

### USAGE

You need to edit file settings.ini before running main.py. Parameter documentation is provided in the file.

### Dry run

By default, the script will only simulate the process and will not perform any changes. To perform changes, you need to set the flag "dry_run" on settings.ini to 0.

### Offline mode

In offline mode, the script will not import any data to Neo4j, not even try to communicate with it. It will only export the csv files plus a cypher script to import them. This way, you may copy these files to another machine and do the import. As the script is in Cypher, the target machine needs nothing but Neo4j installed. *Note that, in that case, it is better to specify the directory where these files will be on the target machine (there is an option for that in settings.ini).*

### Data type support
 - LOBs are not supported by Neo4j and, thus, not supported by the script at the moment.
 - All other standard sql types are supported.
 - Vendor-specific data types (e.g. SMALLMONEY, INTERVAL etc.) are, in theory, supported, but *no tests have been performed. If you encounter such types, please provide feedback.*

### Tests so far

So far, the script has migrated:
 - SAKILA sample database on MySQL.
 - Northwind sample database on MySQL.

### Features under consideration

 - _Offline mode_: extract CSVs to a convenient directory. Create a cypher script to import them and create relations, in order to run it anytime, on a different machine.
 - _User edit mode_: output a file describing the actions that will be performed (maybe in YAML format) that can be modified by the user. That way, the user will be able to rename relationship ACTOR\_FILM to PLAYS\_IN
 - _Some way to handle LOBs_: maybe save LOBs as binary files and store the file's URI as string.
 - ... ?

### LICENSE
---
Copyright (c) 2014, Ioannis Theodosis

Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.


