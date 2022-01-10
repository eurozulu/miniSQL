# TinyDb
## A simple, memory based, SQL database with a tiny footprint.  

Aimed at getting SQL functionality when a full blown RDB is unsuitable.  
With an executable of only 2 Mb, this tiny little database performs many of the basic tasks of its far larger counterparts.  

### Usage  
TinyDb has its own command line interface (CLI) to interact with the user.  
The CLI shows a `>` prompt, where commands may be typed.  
The name of the last loaded database, if any, will appear before the prompt.  

To start the CLI, type:  
`tinydb`  
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

#### INSERT
`INSERT INTO <table name> (<column name> [,<column name>...]) VALUES (<value|NULL>[,<value|NULL>...])`   

#### UPDATE
`UPDATE <table name> SET <colmnname>=<value|NULL> [,<colmnname>=<value|NULL>...] [WHERE <colmnname>=<value|NULL>]`  

#### DELETE
`DELETE FROM <table name> [WHERE <colmnname>=<value|NULL>]`  
  
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
