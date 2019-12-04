
Privex's Python Database Library
================================

[![Build Status](https://travis-ci.com/Privex/python-db.svg?branch=master)](https://travis-ci.com/Privex/python-db) 
[![Codecov](https://img.shields.io/codecov/c/github/Privex/python-db.svg)](https://codecov.io/gh/Privex/python-db)
[![PyPi Version](https://img.shields.io/pypi/v/privex-db.svg)](https://pypi.org/project/privex-db/)
![License Button](https://img.shields.io/pypi/l/privex-db) 
![PyPI - Downloads](https://img.shields.io/pypi/dm/privex-db)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/privex-db) 
![GitHub last commit](https://img.shields.io/github/last-commit/Privex/python-db)


```
+===================================================+
|                 © 2019 Privex Inc.                |
|               https://www.privex.io               |
+===================================================+
|                                                   |
|        Privex's Python Database Library           |
|        License: X11 / MIT                         |
|                                                   |
|        Originally Developed by Privex Inc.        |
|        Core Developer(s):                         |
|                                                   |
|          (+)  Chris (@someguy123) [Privex]        |
|                                                   |
+===================================================+

Privex's Python Database Library - Database wrappers, query builders, and other useful DB-related classes/functions
Copyright (c) 2019     Privex Inc.   ( https://www.privex.io )
```

README under construction.

# Install with pip

We recommend at least Python 3.6 - we cannot guarantee compatibility with older versions.

```
pip3 install privex-db
```

# Basic Usage

Basic usage with `SqliteWrapper` and `SqliteBuilder` (`db.builder()`)

```python
from os.path import expanduser
from privex.db import SqliteWrapper

# Open or create the database file ~/.my_app/my_app.db
db = SqliteWrapper(expanduser("~/.my_app/my_app.db"))

# Create the table 'items' and insert some items
db.action("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);")
db.action("INSERT INTO items (name) VALUES (?);", ["Cardboard Box"])
db.action("INSERT INTO items (name) VALUES (?);", ["Orange"])
db.action("INSERT INTO items (name) VALUES (?);", ["Banana"])
db.action("INSERT INTO items (name) VALUES (?);", ["Stack of Paper"])

item = db.fetchone("SELECT * FROM items WHERE name = ?", ['Orange'])

print(item.id, '-', item.name)
# Output: 2 - Orange

q = db.builder('items')
# Privex QueryBuilder's support chaining similar to Django's ORM
q.select('id', 'name') \           # SELECT id, name FROM items
    .where('name', 'Orange') \     # WHERE name = 'Orange'
    .where_or('name', 'Banana') \  # OR name = 'Banana'
    .order('name', 'id')           # ORDER BY name, id DESC

# You can either iterate directly over the query builder object
for row in q:
    print(f"ID: {row.id}   Name: {row.name}")
# Output:
# ID: 3   Name: Banana
# ID: 2   Name: Orange

# Or you can use .fetch / .all to grab a single row, or all rows as a list
item = db.builder('items').where('name', 'Orange').fetch()
# {'id': 2, 'name': 'Orange'}
items = db.builder('items').all()
# [ {'id': 1, 'name': 'Cardboard Box'}, {'id': 2, 'name': 'Orange'}, ... ]
```


# Documentation

[![Read the Documentation](https://read-the-docs-guidelines.readthedocs-hosted.com/_images/logo-wordmark-dark.png)](
https://privex-db.readthedocs.io/en/latest/)

Full documentation for this project is available above (click the Read The Docs image), including:

 - How to install the application and it's dependencies 
 - How to use the various functions and classes
 - General documentation of the modules and classes for contributors

**To build the documentation:**

```bash
pip3 install pipenv
git clone https://github.com/Privex/python-db
cd python-db/docs
pipenv install -d

# It's recommended to run make clean to ensure old HTML files are removed
# `make html` generates the .html and static files in docs/build for production
make clean && make html

# After the files are built, you can live develop the docs using `make live`
# then browse to http://127.0.0.1:8100/
# If you have issues with content not showing up correctly, try make clean && make html
# then run make live again.
make live
```


Unit Tests
===========

To run the unit tests, clone the project and make a `.env` file containing details for a PostgreSQL database
(for the Postgres wrapper + builder tests).

```
DB_USER=yourname
DB_NAME=privex_py_db
DB_BACKEND=postgresql
LOG_LEVEL=DEBUG
```

Install all required dependencies:

```
pip3 install pipenv
pipenv install -d
```

Now run the tests (-v for more detailed testing output):

```
pipenv run pytest -rxXs -v
```

