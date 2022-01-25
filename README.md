# SimpleDB

SimpleDB is a basic Database Management System, written in Java and developed for didactic purposes. 
This project focuses on implementing core modules required for a DBMS which includes 
* modules required to store and retrieve data on disk
* transaction support
* locking support
* query processing operators for execution

Have a look at the [documentation](https://hrily.github.io/SimpleDB/) for more.

## Features

- [X] support for fields, tuples, and tuple schemas
- [X] support for predicates and conditions to tuples
- [X] support for one or more access methods (e.g., heap files) that store relations on disk and provide a way to iterate through tuples of those relations
- [X] support for operators (e.g., select, join, insert, delete, etc.) that process tuples
- [X] buffer pool that caches active tuples and pages in memory
- [X] catalog that stores information about available tables and their schemas.