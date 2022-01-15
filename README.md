# MiniSQL
## A simple, memory based, SQL database with a tiny footprint.  

Aimed at getting SQL functionality when a full blown RDB is unsuitable.  
With an executable under 3 Mb, this tiny little database performs many of the basic tasks of its far larger counterparts.  

### Usage  
MiniSQL has its own command line interface (CLI) to interact with the user.  
The CLI shows a `>` prompt, where commands may be typed.  
The name of the last loaded database, if any, will appear before the prompt.  

To start the CLI, type:  
`minisql`  
from the (OS) command line.  

At the `>` prompt type any SQL command to create new tables and insert data into them.  
e.g. `CREATE TABLE myfirsttable (col1, col2, col3)`  
To see the new table type:  
`DESC myfirsttable` or `TABLES` to list the available tables.   

For a short guide on available commands, type `help`.  

The interface supports command history to save retyping common commands.  
Using the up and down arrows to scroll through previous commands.  
  
In addition to the CLI, commands can be pipped in via the standard input.  
This allows 'scripts' to be predefined and passed into the database without typing them in one line at a time.  


### Queries  
Supported queries are:  
* `SELECT`
* `INSERT`
* `UPDATE`
* `DELETE`  
  
#### SELECT  
`SELECT <column name> [,<column name>...] [INTO <table name>] FROM <table name> [WHERE <colmnname>=<value|NULL>]`  
Column names should be columns in the named table.  Use wildcard `*` to select all columns  
INTO is an optional name of a new table to insert the results into.  The table must NOT exist.  
FROM is a required keyword followed by the name of the table to select from.  Table must exist in the current database.  
WHERE is an optional set of filter conditions to limit the selected values.  See [Where](#WHERE)

#### INSERT
`INSERT INTO <table name> (<column name> [,<column name>...]) VALUES (<value|NULL>[,<value|NULL>...])`  
or  
`INSERT INTO <table name> (<column name> [,<column name>...]) SELECT <column name> [,<column name>...] FROM <table name> [WHERE <colmnname>=<value|NULL>]`  
Insert has two forms, VALUES and SELECT.  VALUES inserts a single record of the given values, SELECT inserts all the results of the given SELECT query.  
`INTO`  a required keyword followed by the table name of where to insert the new records.  
(col[,col...]) A required, bracketed, list of column name of where to insert the new data.  must be valid columns in the table.  
`VALUES` or `SELECT`  Required keyword followed by the Values or select query.  

Values should by bracketed, comma delmited list of values with the corrisponding number of elements to match the columns named in the query.  
To insert a NULL value, use the `NULL` keyword, e.g. (1,2,NULL)  
  
SELECT query should be a valid [SELECT](#SELECT) query (Without its own INTO)  


#### UPDATE
`UPDATE <table name> SET <colmnname>=<value|NULL> [,<colmnname>=<value|NULL>...] [WHERE <colmnname>=<value|NULL>]`  
Update changes values of an existing record.  
`<table name>` a required name of an existing table.  
`SET` a required keyword followed by one or more assignments.  
assignments are a column name and a value, seperated with an '='  
additional assignments can be listed using a comma delimiter.  
.e.g.  `SET mycol = 1, myothercol = 'haha'`  
WHERE is an optional set of filter conditions to limit the updated values.  See [Where](#WHERE)


#### DELETE
`DELETE FROM <table name> [WHERE <colmnname>=<value|NULL>]`  
Deletes records from the table.  
`FROM` a required keyword, followed by the table name to delete from.  
WHERE is an optional set of filter conditions to limit the deleted values.  See [Where](#WHERE)


#### WHERE  
The Where clause is used to filter results in SELECT, UPDATE or DELETE.  
Where consists of one or more 'conditions', linked using operators `AND` or `OR`.  
Each condition begins with a column name followed by an operator, followed by the comparison value.  
e.g. `mycol = 'haha'` or `mycol <= 3 AND myothercol != NULL`  
  
Supported operators are:  
* `=`
* '!=' or '<>'
* '>='
* '<='
* '>'
* '<'
* 'LIKE'
* 'BETWEEN' *not yet supported  
  
Conditions may be preceeded with `NOT` to invert the condition outcome.  
e.g. `NOT mycol = 'haha'` or `mycol <= 3 AND NOT myothercol = NULL`  
  
Conditions may use brackets to define complex conditions.  
e.g. `(col1 = true OR col2 = true) AND col3 > 0`  
Where the bracketed conditions are evaluated as a single result, prior to the condition outside the brackets.  

### Commands
Supported commands to manipulate the database schema are:  
* CREATE
* DROP
  
#### CREATE
`CREATE TABLE <table name> (<column name> [, <column name>...])`  
e.g. `CREATE TABLE mytable (col1, col2)`  
Creates a new table called mytables with two columns

`CREATE COLUMN | COL <table name> (<column name> [, <column name>...])`  
e.g. `CREATE COLUMN mytable (col3, col4)`  
Adds two new columns to the 'mytable' existing table

#### DROP
`DROP TABLE <table name>`  
e.g. `DROP TABLE mytable`  
Deletes the existing table 'mytable'

`DROP COLUMN | COL <table name> (<column name> [, <column name>...])`  
e.g. `DROP COLUMN mytable (col2, col4)`  
Deletes the two columns from the existing 'mytable' table  
  
`DROP DATABASE`  
Drops the entire database.  All tables are deleted, leaving the database empty.
  

### Database
The current database is loosly define by the tables which are currently loaded.  
Performing multiple `RESTORE` commands will merge all the tables from each file, into one single 'database'  
The last `RESTORE` defines the 'name' given to the database, which can be seen before the CLI prompt.  
  
To view the current database state there are two commands:  
* `TABLES`
* `DESCRIBE | DESC`  
#### TABLES
`TABLES` has no parameters.As you might guess, lists all the table names in the database.  

#### DESC
`DESC | DESCRIBE <table name>` lists the column names of a named table.  

### Persistence
The database state can be saved to, and restored from disk using the two commands:  
* `DUMP`
* `RESTORE`  

#### DUMP
`DUMP <filename of where to save dump file>`
Filename is required. If no file extension is given, `.json` is added.  
  

#### RESTORE
`RESTORE <filename of where to load dump file>`
Filename is required. If no file extension is given, `.json` is added.  

When restoring, existing tables are not dropped, so the new database is merged with the existing one.  
Use `DROP DATABASE` prior to `RESTORE` to ensure database only has the tables in the dump file.  
