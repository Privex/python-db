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
import asyncio
import os
import sqlite3
import logging
import warnings
from os.path import expanduser, join, dirname, isabs
from typing import List, Tuple, Optional, Any, Union, Set, Iterable

from async_property import async_property
from privex.helpers import empty, DictObject, is_namedtuple, empty_if

from privex.db.base import GenericDBWrapper, GenericAsyncDBWrapper, _should_zip, cursor_to_dict, DBExecution
from privex.db.query.sqlite import SqliteQueryBuilder
from privex.db.query import SqliteAsyncQueryBuilder
from privex.db.types import GenericAsyncCursor

log = logging.getLogger(__name__)


def parse_db_args(ins, db=None, memory_persist=False, connection_kwargs=None, default_kwargs=None):
    connection_kwargs = empty_if(connection_kwargs, {})
    default_conn_kwargs = empty_if(
        default_kwargs, dict(isolation_level=ins.isolation_level, timeout=ins.db_timeout)
    )
    
    db = 'file::memory:?cache=shared' if memory_persist else empty_if(db, ins.DEFAULT_DB)
    
    if ':memory:' not in db:
        db_folder = dirname(db)
        if not isabs(db):
            log.debug("Passed 'db' argument isn't absolute: %s", db)
            db = join(ins.DEFAULT_DB_FOLDER, db)
            log.debug("Prepended DEFAULT_DB_FOLDER to 'db' argument: %s", db)
            db_folder = dirname(db)
        
        if not os.path.exists(db_folder):
            log.debug("Database folder '%s' doesn't exist. Creating it + any missing parent folders", db_folder)
            os.makedirs(db_folder)
    else:
        log.debug("Passed 'db' argument is %s - using in-memory sqlite3 database.", db)
        if 'file:' in db:
            default_conn_kwargs['uri'] = True
    
    return db, {**default_conn_kwargs, **connection_kwargs}


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
        
        :key bool memory_persist: Use a shared in-memory database, which can be accessed by other instances of
                                  this class (in this process) - which is cleared after all memory
                                  connections are closed.
                                  Shortcut for ``db='file::memory:?cache=shared'``
        
        .. _Python SQLite3 Docs: https://docs.python.org/3.8/library/sqlite3.html#sqlite3.Connection.isolation_level
        
        """
        self.isolation_level = isolation_level
        self.db_timeout = int(kwargs.pop('db_timeout', 30))
        self.query_mode = kwargs.pop('query_mode', 'dict')
        self._conn = None
        self._builder = None
        
        memory_persist = kwargs.pop('memory_persist', False)

        db, conn_kwargs = parse_db_args(
            self, db, memory_persist=memory_persist, connection_kwargs=kwargs.pop('connection_kwargs', {})
        )

        self.db = db

        # conn_kwargs = {**default_conn_kwargs, **kwargs.pop('connection_kwargs', {})}
        super().__init__(
            db=db, connector_func=sqlite3.connect, connector_args=[db], query_mode=self.query_mode,
            connector_kwargs=conn_kwargs,
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


try:
    import aiosqlite
    
    class SqliteAsyncWrapper(GenericAsyncDBWrapper):
        """
        
        **Usage**
        
        Creating an instance::
            
            >>> from privex.db import SqliteAsyncWrapper
            >>> db = SqliteAsyncWrapper('my_app.db')

        Inserting rows::
            
            >>> db.insert('users', first_name='John', last_name='Doe')
            >>> db.insert('users', first_name='Dave', last_name='Johnson')
        
        Running raw queries::
            
            >>> # fetchone() allows you to run a raw query, and a dict is returned with the first row result
            >>> row = await db.fetchone("SELECT * FROM users WHERE first_name = ?;", ['John'])
            >>> row['first_name']
            John
            >>> row['last_name']
            Doe
            
            >>> # fetchall() runs a query and returns an iterator of the returned rows
            >>> rows = await db.fetchall("SELECT * FROM users;")
            >>> for user in rows:
            ...     print(f"First Name: {row['first_name']}   ||    Last Name: {row['last_name']}")
            ...
            First Name: John   ||    Last Name: Doe
            First Name: Dave   ||    Last Name: Johnson
            
            >>> # action() is for running queries where you don't want to fetch any results. It simply returns the
            >>> # affected row count as an integer.
            >>> row_count = await db.action('UPDATE users SET first_name = ? WHERE id = ?;', ['David', 2])
            >>> print(row_count)
            1

        Creating tables if they don't already exist::
            
            >>> # If the table 'users' doesn't exist, the CREATE TABLE query will be executed.
            >>> await db.create_schema(
            ...    'users',
            ...    "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, first_name TEXT, last_name TEXT);"
            ... )
            >>>
        
        Using the query builder::
            
            >>> # You can either use it directly
            >>> q = db.builder('users')
            >>> q.select('first_name', 'last_name').where('first_name', 'John').where_or('last_name', 'Doe')
            >>> results = q.all()
            >>> async for row in results:
            ...     print(f"First Name: {row['first_name']}   ||    Last Name: {row['last_name']}")
            ...
            First Name: John   ||    Last Name: Doe
            
            >>> # Or, you can use it in a ``with`` statement to maintain a singular connection, which means you
            >>> # can use .fetch_next to fetch a singular row at a time (you can still use .all() and .fetch())
            >>> async with db.builder('users') as q:
            ...     q.select('first_name', 'last_name')
            ...     row = q.fetch_next()
            ...     print('Name:', row['first_name'], row['last_name'])   # John Doe
            ...     row = q.fetch_next()
            ...     print('Name:', row['first_name'], row['last_name'])   # Dave Johnson
            ...
            Name: John Doe
            Name: Dave Johnson
            
        
        
        Creating a wrapper sub-class of SqliteAsyncWrapper:
        
        
        .. code-block:: python
            
            class MyManager(SqliteAsyncWrapper):
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
                
                async def get_items(self):
                    # This is an example of a helper method you might want to define, which simply calls
                    # self.fetchall with a pre-defined SQL query
                    return await self.fetchall("SELECT * FROM items;")
                
                async def find_item(self, id: int):
                    # This is an example of a helper method you might want to define, which simply calls
                    # self.fetchone with a pre-defined SQL query, and interpolates the 'id' parameter into
                    # the prepared statement.
                    return await self.fetchone("SELECT * FROM items WHERE id = ?;", [id]);
        
        """
        AIO_CUR = aiosqlite.Cursor
    
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
    
        _conn: Optional[aiosqlite.Connection]
        """Instance variable which holds the current SQLite3 connection object"""
    
        _builder: Optional[SqliteAsyncQueryBuilder]

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

            self.isolation_level = isolation_level
            self.db_timeout = int(kwargs.pop('db_timeout', 30))
            self.query_mode = kwargs.pop('query_mode', 'dict')
            self._conn = None
            self._builder = None
            
            memory_persist = kwargs.pop('memory_persist', False)

            db, conn_kwargs = parse_db_args(
                self, db, memory_persist=memory_persist, connection_kwargs=kwargs.pop('connection_kwargs', {})
            )

            # db = self.DEFAULT_DB if db is None else db
            # if ':memory:' not in db:
            #     db_folder = dirname(db)
            #     if not isabs(db):
            #         log.debug("Passed 'db' argument isn't absolute: %s", db)
            #         db = join(self.DEFAULT_DB_FOLDER, db)
            #         log.debug("Prepended DEFAULT_DB_FOLDER to 'db' argument: %s", db)
            #         db_folder = dirname(db)
            #
            #     if not os.path.exists(db_folder):
            #         log.debug("Database folder '%s' doesn't exist. Creating it + any missing parent folders", db_folder)
            #         os.makedirs(db_folder)
            # else:
            #     log.debug("Passed 'db' argument is :memory: - using in-memory sqlite3 database.")
            self.db = db
            
            super().__init__(
                db=db, connector_func=aiosqlite.connect, connector_args=[db], query_mode=self.query_mode,
                connector_kwargs=conn_kwargs, **kwargs
            )

        async def get_cursor(self, cursor_name=None, cursor_class=None, *args, **kwargs) -> aiosqlite.Cursor:
            # Disable cursor_mgr by default, as aiosqlite already has a context manager.
            kwargs = dict(kwargs)
            kwargs['cursor_mgr'] = kwargs.pop('cursor_mgr', False)
            # noinspection PyTypeChecker
            return await super().get_cursor(cursor_name=cursor_name, cursor_class=cursor_class, *args, **kwargs)

        @async_property
        async def cursor(self) -> aiosqlite.Cursor:
            # if self._cursor is None:
            #     self._cursor = self.get_cursor(cursor_mgr=False, close_callback=self._close_callback)
            # if asyncio.iscoroutine(self._cursor):
            #     self._cursor = await self._cursor
            return await self.get_cursor(cursor_mgr=False, close_callback=self._close_callback)
        
        # noinspection PyTypeChecker
        @async_property
        async def conn(self) -> aiosqlite.Connection:
            """Get or create an SQLite3 connection using DB file :py:attr:`.db` and return it"""
            return await super().conn  # type: aiosqlite.Connection
        
        # noinspection PyTypeChecker
        def builder(self, table: str) -> SqliteAsyncQueryBuilder:
            return SqliteAsyncQueryBuilder(
                table=table, connection_args=self.connector_args, connection_kwargs=self.connector_kwargs
            )

        async def _get_cursor(self, cursor_name=None, cursor_class=None, *args, **kwargs):
            # conn = await self.conn
            # return conn.cursor()
            return await self._get_connection(new=True, await_conn=False)

        # noinspection PyTypeChecker
        async def insert(self, _table: str, _cursor: AIO_CUR = None, **fields) -> Union[DictObject, AIO_CUR]:
            return await super().insert(_table, _cursor, **fields)

        _Q_OUT_TYPE = GenericAsyncDBWrapper._Q_OUT_TYPE
        
        # async def _query(self, sql: str, *params, fetch='all', **kwparams) -> _Q_OUT_TYPE:
        #     conn = await self.conn
        #     async with conn as db:
        #         query_mode = kwparams.pop('query_mode', self.query_mode)
        #         async with db.execute(sql, *params, **kwparams) as cur:
        #             if fetch == 'all':
        #                 if self.AUTO_ZIP_COLS and query_mode == 'dict':
        #                     res = [self._zip_cols(cur, r) for r in cur]
        #             elif fetch == 'one':
        #                 res = cur[0]
        #                 if res is None:
        #                     return None, cur, cursor_to_dict(cur)
        #                 if _should_zip(res, query_mode=query_mode, auto_zip=self.AUTO_ZIP_COLS):
        #                     res = self._zip_cols(cur, tuple(res))
        #             elif fetch == 'no':
        #                 res = None
        #             else:
        #                 raise AttributeError("The parameter 'fetch' must be either 'all', 'one' or 'no'.")
        #             if self.enable_execution_log:
        #                 self._execution_log += [DBExecution(sql, res, cur, cursor_to_dict(cur))]
        #             return res, cur, cursor_to_dict(cur)
        
        async def execute(self, query: str, *params: Iterable, fetch='all', **kwargs) \
                -> Tuple[Iterable, DictObject]:
            
            # cursor_name = kwargs.pop('cursor_name', None)
            cleanup_cursor = kwargs.pop('cleanup_cursor', True)
            _cur: aiosqlite.Connection = kwargs.pop('cursor', None)
            res = None

            # cur = _cur
            if _cur is None:
                # noinspection PyTypeChecker
                _cur: aiosqlite.Connection = await self._get_connection(new=True, await_conn=False)
            
            # if not cleanup_cursor:
            #     # _cur.
            #     cur = await _cur.execute(query, *params)
            #     if fetch == 'all': res = await cur.fetchall()
            #     if fetch == 'one': res = await cur.fetchone()
            #     return res, cur
            
            async with _cur as conn:
                async with conn.execute(query, *params) as cur:
                    if fetch == 'all': res = await cur.fetchall()
                    if fetch == 'one': res = await cur.fetchone()
                    cur_dict = cursor_to_dict(cur)
                await conn.commit()
                return res, cur_dict

        def __enter__(self):
            self._conn = self.conn
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            await self.close_cursor()
            if self._conn is not None:
                conn = await self._conn
                await conn.close()
                del self._conn
            self._conn = None
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            self._conn = None

except ImportError:
    warnings.warn("Could not import 'aiosqlite'. SqliteAsyncWrapper will not be available.")

