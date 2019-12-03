######################
Examples / Basic Usage
######################

Using the SQLite3 Manager + Query Builder
=========================================

Basic / direct usage of SqliteWrapper
-------------------------------------

.. code-block:: python

    from os.path import expanduser
    from typing import List, Tuple
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


Using the query builder (SqliteQueryBuilder)
--------------------------------------------

Once you have an instance of :class:`.SqliteWrapper`, you can easily create query builders via the ``.builder``
function.

Privex-DB query builders work similarly to Django's ORM, and are very simple to use.

.. code-block:: python

    q = db.builder('items')
    # Privex QueryBuilder's support chaining similar to Django's ORM
    q.select('id', 'name') \           # SELECT id, name
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


Sub-classing SqliteWrapper for your app
---------------------------------------

To make the most out of the wrapper classes, you'll want to create a sub-class which is tuned for your application,
including the table schemas that your application needs.

**NOTE**: :class:`.SqliteWrapper` runs in auto-commit mode by default. If you don't want to use auto-commit, you
can pass ``isolation_level=XXX`` to the constructor to choose a custom isolation level without autocommit. See
the `Python SQLite3 Docs`_ for more information on isolation modes.

.. _Python SQLite3 Docs: https://docs.python.org/3.8/library/sqlite3.html#sqlite3.Connection.isolation_level

Below is an example of sub-classing :class:`.SqliteWrapper` to create two tables (``users`` and ``items``), with
some custom helper methods, then instantiating the class, inserting some rows, and querying them.

.. code-block:: python

    from os.path import expanduser, join
    from typing import List, Tuple
    from privex.db import SqliteWrapper

    class MyDBWrapper(SqliteWrapper):
        ###
        # If a database path isn't specified, then the class attribute DEFAULT_DB will be used.
        ###
        DEFAULT_DB_FOLDER: str = expanduser('~/.my_app')
        DEFAULT_DB_NAME: str = 'my_app.db'
        DEFAULT_DB: str = join(DEFAULT_DB_FOLDER, DEFAULT_DB_NAME)

        ###
        # The SCHEMAS class attribute contains a list of tuples, with each tuple containing the name of a
        # table, as well as the SQL query required to create the table if it doesn't exist.
        ###
        SCHEMAS: List[Tuple[str, str]] = [
            ('users', "CREATE TABLE users ("
                      "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                      "first_name TEXT, "
                      "last_name TEXT, "
                      "address TEXT NULL"
                      ");"),
            ('items', "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);"),
        ]

        def get_items(self):
            # This is an example of a helper method you might want to define, which simply calls
            # self.fetchall with a pre-defined SQL query
            return self.fetchall("SELECT * FROM items;")

        def find_item(self, id: int):
            # This is an example of a helper method you might want to define, which simply calls
            # self.fetchone with a pre-defined SQL query, and interpolates the 'id' parameter into
            # the prepared statement.
            return self.fetchone("SELECT * FROM items WHERE id = ?;", [id])

        def get_users(self): return self.fetchall("SELECT * FROM users;")

        def find_user(self, id: int): return self.fetchall("SELECT * FROM users WHERE id = ?;", [id])

    # Once the class is constructed, it should've created the SQLite3 database ~/.my_app/my_app.db (if it didn't exist)
    # and then created the tables 'users' and 'items' if they didn't already exist.
    db = MyDBWrapper()

    # The method .action runs a query, but doesn't attempt to fetch rows, it only returns the affected row count
    # Note: By default, SqliteWrapper uses SQLite3 auto-commit mode
    db.action("INSERT INTO users (first_name, last_name) VALUES (?, ?);", ['John', 'Doe'])
    db.action("INSERT INTO users (first_name, last_name, address) VALUES (?, ?, ?);", ['Jane', 'Doe', '123 Ex St'])
    db.action("INSERT INTO users (first_name, last_name) VALUES (?, ?);", ['Dave', 'Johnston'])
    db.action("INSERT INTO users (first_name, last_name) VALUES (?, ?);", ['Aaron', 'Johnston'])

    users = db.get_users()

    for u in users:
        print(f"User: ID {u.id}  /  First Name: {u.first_name}   /   Last Name: {u.last_name}")


If we then run this example, we get the output::

    user@example ~ $ python3 example.py
    User: ID 1  /  First Name: John   /   Last Name: Doe
    User: ID 2  /  First Name: Jane   /   Last Name: Doe
    User: ID 3  /  First Name: Dave   /   Last Name: Johnston
    User: ID 4  /  First Name: Aaron   /   Last Name: Johnston


Using the query builder from your sub-class
-------------------------------------------

We can also use :class:`.SqliteQueryBuilder` directly from our sub-class, which is a primitive ORM for building
and executing SQL queries.

Let's build a slightly complex query to show how powerful it is. We'll build a query to aggregate the number
of users who share a given last name AND don't have an address.

.. code-block:: python

    # Get an SqliteQueryBuilder instance for the table 'users'
    q = db.builder('users')

    # Privex QueryBuilder's support chaining similar to Django's ORM
    q \
        .select('last_name', 'COUNT(last_name) AS total') \
        .where('address', None) \
        .group_by('last_name')

    print(f"\nQuery:\n\t{q.build_query()}\n")
    results = q.all()

    for r in results:
        print('Result:', r)


If we then run this example, we get the output::


    Query:
         SELECT last_name, COUNT(last_name) AS total FROM users WHERE address IS NULL GROUP BY last_name;

    Result: {'last_name': 'Doe', 'total': 1}
    Result: {'last_name': 'Johnston', 'total': 2}


