"""
This module holds :class:`.SqliteWrapper` - a somewhat higher level class for interacting with SQLite3 databases.

**Copyright**::

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

    Copyright (c) 2019     Privex Inc.   ( https://www.privex.io )


"""
import os
import sqlite3
import logging
from os.path import expanduser, join, dirname, isabs
from typing import List, Tuple, Optional, Any, Union, Set
from privex.helpers import empty, DictObject

from privex.db.base import GenericDBWrapper
from privex.db.query.sqlite import SqliteQueryBuilder

log = logging.getLogger(__name__)


class SqliteWrapper(GenericDBWrapper):
    """
    Lightweight wrapper class for interacting with Sqlite3 databases.
    
    **Simple direct class usage**
    
    >>> db_path = expanduser('~/.my_app/my_db.db')
    >>> db = SqliteWrapper(db=db_path)
    >>> users = db.fetchall("SELECT * FROM users;")
    
    **Usage**
    
    Below is an example wrapper class which uses :class:`.SqliteWrapper` as it's parent class.
    
    It overrides the class attributes :py:attr:`.DEFAULT_DB_FOLDER`, :py:attr:`.DEFAULT_DB_NAME`, and
    :py:attr:`.DEFAULT_DB` - so that if no database path is passed to ``MyManager``, then the database file path
    contained in ``MyManager.DEFAULT_DB`` will be used as a default.
    
    It also overrides :py:attr:`.SCHEMAS` to define 2 tables (``users`` and ``items``) which will be automatically
    created when the class is instantiated, unless they already exist.
    
    It adds two methods ``get_items`` (returns an iterator
    
    .. code-block:: python
        
        class MyManager(SqliteWrapper):
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
                ('users', "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);"),
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
                return self.fetchone("SELECT * FROM items WHERE id = ?;", [id]);
            
            
    
    """
    DEFAULT_DB_FOLDER: str = expanduser('~/.privex_sqlite')
    """If an absolute path isn't given, store the sqlite3 database file in this folder"""
    DEFAULT_DB_NAME: str = 'privex_sqlite.db'
    """If no database is specified to :meth:`.__init__`, then use this (appended to :py:attr:`.DEFAULT_DB_FOLDER`)"""
    DEFAULT_DB: str = join(DEFAULT_DB_FOLDER, DEFAULT_DB_NAME)
    """
    Combined :py:attr:`.DEFAULT_DB_FOLDER` and :py:attr:`.DEFAULT_DB_NAME` used as default absolute path for
    the sqlite3 database
    """
    
    DEFAULT_TABLE_QUERY = "SELECT count(name) as table_count FROM sqlite_master WHERE type = 'table' AND name = ?"
    DEFAULT_TABLE_LIST_QUERY = "SELECT name FROM sqlite_master WHERE type = 'table'"
    
    db: str
    """Path to the SQLite3 database for this class instance"""
    
    _conn: Optional[sqlite3.Connection]
    """Instance variable which holds the current SQLite3 connection object"""
    
    _builder: Optional[SqliteQueryBuilder]
    
    def __init__(self, db: str = None, isolation_level=None, **kwargs):
        """
        
        :param str db: Relative / absolute path to SQLite3 database file to use.
        :param isolation_level: Isolation level for SQLite3 connection. Defaults to ``None`` (autocommit).
                                See the `Python SQLite3 Docs`_ for more information.
        
        :key int db_timeout: Amount of time to wait for any SQLite3 locks to expire before giving up
        :key str query_mode: Either ``'flat'`` (query returns tuples) or ``'dict'`` (query returns dicts).
                             More details in PyDoc block under :py:attr:`.query_mode`
        
        .. _Python SQLite3 Docs: https://docs.python.org/3.8/library/sqlite3.html#sqlite3.Connection.isolation_level
        
        """
        db = self.DEFAULT_DB if db is None else db
        if db != ':memory:':
            db_folder = dirname(db)
            if not isabs(db):
                log.debug("Passed 'db' argument isn't absolute: %s", db)
                db = join(self.DEFAULT_DB_FOLDER, db)
                log.debug("Prepended DEFAULT_DB_FOLDER to 'db' argument: %s", db)
                db_folder = dirname(db)
        
            if not os.path.exists(db_folder):
                log.debug("Database folder '%s' doesn't exist. Creating it + any missing parent folders", db_folder)
                os.makedirs(db_folder)
        else:
            log.debug("Passed 'db' argument is :memory: - using in-memory sqlite3 database.")
        self.db = db
        self.isolation_level = isolation_level
        self.db_timeout = int(kwargs.pop('db_timeout', 30))
        self.query_mode = kwargs.pop('query_mode', 'dict')
        self._conn = None
        self._builder = None
        
        super().__init__(
            db=db, connector_func=sqlite3.connect, connector_args=[db], query_mode=self.query_mode,
            connector_kwargs=dict(isolation_level=self.isolation_level, timeout=self.db_timeout),
            **kwargs
        )

    # make_connection: sqlite3.Connection

    # noinspection PyTypeChecker
    @property
    def conn(self) -> sqlite3.Connection:
        """Get or create an SQLite3 connection using DB file :py:attr:`.db` and return it"""
        return super().conn   # type: sqlite3.Connection

    def _get_cursor(self, cursor_name=None, cursor_class=None, *args, **kwargs):
        return super()._get_cursor(cursor_name=cursor_name, cursor_class=cursor_class, *args, **kwargs)

    # noinspection PyTypeChecker
    def builder(self, table: str) -> SqliteQueryBuilder:
        return SqliteQueryBuilder(table=table, connection=self.conn)

    # noinspection PyTypeChecker
    def insert(self, _table: str, _cursor: sqlite3.Cursor = None, **fields) -> Union[DictObject, sqlite3.Cursor]:
        return super().insert(_table, _cursor, **fields)
    
    def __enter__(self):
        self._conn = self.conn
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            self._conn.close()
        self._conn = None
