"""
This module contains core functions/classes which are used across the module, as well as abstract classes / types.

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
import abc
from abc import ABC
from typing import List, Tuple, Set, Union, Any, Optional, TypeVar, Generic
from privex.helpers import dictable_namedtuple, is_namedtuple, empty, DictObject
import logging

from privex.db.types import GenericCursor, GenericConnection

log = logging.getLogger(__name__)
DBExecution = dictable_namedtuple('DBExecution', 'query result execute_result cursor')

CUR = TypeVar('CUR')


class CursorManager(GenericCursor, Generic[CUR], object):
    """
    Not all database API's support context management with their cursors, so this class wraps a given database
    cursor objects, and provides context management methods :meth:`.__enter__` and :meth:`.__exit__`
    """
    _cursor: Union[CUR, GenericCursor, Any]
    """The actual cursor object this class is wrapping"""
    _cursor_id: int
    """The object ID of the cursor instance - for context manager nesting tracking"""
    _close_callback: Optional[callable]
    """The function/method to callback to when the cursor is closed"""
    
    _active_cursor_ids = set()
    """Object IDs of cursors which have a responsible context managing CursorManager"""
    can_cleanup: bool
    """This becomes True if this is the **first** context manager instance for a cursor"""
    is_context_manager: bool
    """``True`` if this class is being used in a ``with`` statement, otherwise ``False``."""
    
    def __init__(self, cursor: CUR, close_callback: Optional[callable] = None):
        """
        Initialise the cursor manager.
        
        :param CUR|GenericCursor cursor: A database cursor object to wrap
        :param callable close_callback: If specified, this callable (function/method) will be called BEFORE and AFTER
                                        the cursor is closed, with the kwargs ``state='BEFORE_CLOSE'`` and
                                        ``state='AFTER_CLOSE'`` respectively.
        """
        self._cursor = cursor
        self._cursor_id = id(cursor)
        self.can_cleanup = False
        self.is_context_manager = False
        self._close_callback = close_callback
        
    @property
    def rowcount(self) -> int: return self._cursor.rowcount

    @property
    def description(self): return self._cursor.description

    @property
    def lastrowid(self): return self._cursor.lastrowid
    
    def execute(self, *args, **kwargs): return self._cursor.execute(*args, **kwargs)
    
    def fetchone(self, *args, **kwargs): return self._cursor.fetchone(*args, **kwargs)

    def fetchall(self, *args, **kwargs): return self._cursor.fetchall(*args, **kwargs)

    def fetchmany(self, *args, **kwargs): return self._cursor.fetchmany(*args, **kwargs)
    
    def close(self, *args, **kwargs):
        if self._close_callback is not None:
            self._close_callback(state='BEFORE_CLOSE')
        _closed = self._cursor.close(*args, **kwargs)
        if self._close_callback is not None:
            self._close_callback(state='AFTER_CLOSE')
            self._close_callback = None
        return _closed
    
    def _cleanup(self, *args, **kwargs):
        if self._cursor is None: return
        
        try:
            _closed = self.close(*args, **kwargs)
            self._cursor = None
            del self._cursor
            return _closed
        except (Exception, BaseException):
            pass
        try:
            del self._cursor
        except AttributeError:
            pass
        return None
    
    def __getattr__(self, item):
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            return getattr(self._cursor, item)

    def __setattr__(self, key, value):
        # if hasattr(self, key):
        try:
            object.__setattr__(self, key, value)
        except AttributeError:
            setattr(self._cursor, key, value)
    
    def __next__(self): return self._cursor.__next__()

    def __iter__(self): return self._cursor.__iter__()

    def __getitem__(self, item): return self._cursor.__getitem__(item)

    def __setitem__(self, item, value): return self._cursor.__setitem__(item, value)

    def __enter__(self):
        self.is_context_manager = True
        self.can_cleanup = self._cursor_id not in self._active_cursor_ids
        if self.can_cleanup:
            self._active_cursor_ids.add(self._cursor_id)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.can_cleanup:
            self._cleanup()
            self._active_cursor_ids.remove(self._cursor_id)
    
    # def __del__(self):
    #     if not self.can_cleanup:
    #         return
    #     try:
    #         self._cleanup()
    #     except:
    #         try:
    #             del self._cursor
    #         except AttributeError:
    #             pass


class GenericDBWrapper(ABC):
    """
    This is a generic database wrapper class, which implements various methods such as:
    
    * Querying methods such as :meth:`.query`, :meth:`.fetch`, :meth:`.fetchone`, :meth:`.fetchall`
    * Table management functions such as  :meth:`.create_schemas`, :meth:`.drop_schemas` and :meth:`.drop_table`
    * Connection and cursor methods / properties: :attr:`.conn`, :meth:`.get_cursor`
    
    While this class is intended to be subclassed by DBMS-specific wrapper classes, all methods follow
    the Python DB API (PEP 249) and the ANSI SQL standard, meaning very little modification is actually required
    to adapt this wrapper to most database systems.
    
    See :class:`.PostgresWrapper` and :class:`.SqliteWrapper` for implementation examples.
    
    """
    SCHEMAS: List[Tuple[str, str]] = []
    """
    This should be set as a class attribute to a list of two value tuples, each containing the name of a table,
    and the SQL query to create the table if it doesn't exist.

    Example::

        SCHEMAS = [
            (
                'my_table',
                "CREATE TABLE my_table (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);"
            ),
            (
                'other_table',
                "CREATE TABLE other_table (id INTEGER PRIMARY KEY AUTOINCREMENT, example TEXT);"
            ),
        ]

    """
    
    DEFAULT_QUERY_MODE: str = 'dict'
    """
    This attribute can be overridden on your inheriting wrapper class to change the default query mode used
    if one isn't specified in the constructor.
    """
    
    DEFAULT_ENABLE_EXECUTION_LOG: bool = True
    
    AUTO_ZIP_COLS: bool = True
    """
    If your database API doesn't support returning rows as dicts or a dict-like structure (e.g. sqlite3),
    then when this setting is enabled, :meth:`._zip_cols` will be called to zip each row with the result column names
    into a dictionary.
    
    If your database API supports returning rows as a dictionary either by default, or via a cursor/connection class
    (e.g. PostgreSQL with psycopg2) then you should set this to ``False`` and use the cursor/connection class instead.
    """
    
    tables_created: Set[str] = set()
    """
    This is a static class attribute which tracks which tables have already been created,
    avoiding :meth:`.create_schemas` having to make as many queries every time the class is constructed.
    """

    query_mode: str
    """
    Per-instance attribute, either:

    * ``'flat'`` (:meth:`.query` returns rows as tuples)
    * ``'dict'`` (:meth:`.query` returns rows as dicts mapping column names to values)

    """

    DEFAULT_TABLE_QUERY = ""
    DEFAULT_TABLE_LIST_QUERY = ""
    table_query: str
    table_list_query: str
    _execution_log: List[DBExecution]
    """Per-instance list of :class:`.DBExecution`'s from each call to :meth:`.query`"""

    def __init__(self, db=None, connector_func: callable = None, **kwargs):
        """
        Initialise the database wrapper class.
        
        This constructor sets ``_conn`` to None, and sets up various instance variables such as ``connector_func``
        and ``query_mode``.
        
        While you can set various instance variables such as ``query_mode`` via this constructor, if you're inheriting
        this class, it's recommended that you override the ``DEFAULT_`` static class attributes to your preference.
        
        
        :param str db: The name / path of the database to connect to
        :param callable connector_func: A function / method / lambda which returns a database connection object
                                        which acts like :class:`.GenericConnection`
        :key bool auto_create_schema: (Default: ``True``) If True, call :meth:`.create_schemas` during constructor.
        :key list connector_args: A list of arguments to be passed as positional args to ``connector_func``
        :key dict connector_kwargs: A dict of arguments to passed as keyword args to ``connector_func``
        :key str query_mode: Either ``flat`` (return tuples) or ``dict`` (return dicts of column:value)
                             Controls how results are returned from query functions,
                             e.g. :py:meth:`.query` and :py:meth:`.fetch`
        :key str table_query: The query used to check for existence of a table. The query should take one prepared
                              statement argument (the table name to check for), and the first column returned
                              must be named ``table_count`` - an integer containing how many tables were found under
                              the given name (usually just 0 if not found, 1 if found).
        :key str table_list_query: The query used to obtain a list of tables in the database.
                                   The query should take no arguments, and return rows containing one column each,
                                   ``name`` - the name of the table.
        """
        self.db = db
        self._conn = None
        self._cursor = None
        self._execution_log = []
        self._cursors = []
        self.connector_func = connector_func
        auto_create_schema = kwargs.pop('auto_create_schema', True)
        self.enable_execution_log = kwargs.pop('enable_execution_log', self.DEFAULT_ENABLE_EXECUTION_LOG)
        self.connector_args = kwargs.pop('connector_args', [])
        self.connector_kwargs = kwargs.pop('connector_kwargs', {})
        self.query_mode = kwargs.pop('query_mode', self.DEFAULT_QUERY_MODE)
        self.table_query = kwargs.pop('table_query', self.DEFAULT_TABLE_QUERY)
        self.table_list_query = kwargs.pop('table_list_query', self.DEFAULT_TABLE_LIST_QUERY)

        if auto_create_schema:
            res = self.create_schemas()
            log.debug('Create schema result: "%s"', res)
        super().__init__()

    def make_connection(self, *args, **kwargs) -> GenericConnection:
        """
        Creates a database connection using :py:attr:`.connector_func`, passing all arguments/kwargs directly
        to the connector function.
        
        :return GenericConnection conn: A database connection object, which should implement at least the basic
                                        connection object methods as specified in the Python DB API (PEP 249),
                                        and in the Protocol type class :class:`.GenericConnection`
        """
        return self.connector_func(*args, **kwargs)

    @property
    def conn(self) -> GenericConnection:
        """Get or create a database connection"""
        if self._conn is None:
            self._conn = self.make_connection(*self.connector_args, **self.connector_kwargs)
        return self._conn

    def table_exists(self, table: str) -> bool:
        """
        Returns ``True`` if the table ``table`` exists in the database, otherwise ``False``.
        
        
            >>> GenericDBWrapper().table_exists('some_table')
            True
            >>> GenericDBWrapper().table_exists('other_table')
            False
        
        
        :param str table: The table to check for existence.
        :return bool exists: ``True`` if the table ``table`` exists in the database, otherwise ``False``.
        """
        
        res = self.fetchone(self.table_query, [table])
        if isinstance(res, dict):
            return res['table_count'] == 1
        else:
            return res[0] == 1

    def list_tables(self) -> List[str]:
        """
        Get a list of tables present in the current database.
        
        Example::
        
            >>> GenericDBWrapper().list_tables()
            ['sqlite_sequence', 'nodes', 'node_api', 'node_failures']
        
        
        :return List[str] tables: A list of tables in the database
        """
        res = self.fetchall(self.table_list_query)
        if len(res) < 1:
            return []
        if isinstance(res[0], dict):
            return [r['name'] for r in res]
        else:
            return [r[0] for r in res]

    def query(self, sql: str, *params, fetch='all', **kwparams) -> Tuple[Optional[iter], Any, GenericCursor]:
        """
        
        Create an instance of your database wrapper:
        
            >>> db = GenericDBWrapper()
        
        **Querying with prepared SQL queries and returning a single row**::
        
            >>> res, res_exec, cur = db.query("SELECT * FROM users WHERE first_name = ?;", ['John'], fetch='one')
            >>> res
            (12, 'John', 'Doe', '123 Example Road',)
            >>> cur.close()
        
        **Querying with plain SQL queries, using query_mode, handling an iterator result, and using the cursor**
        
        If your database API returns rows as ``tuple``s or ``list``s, you can use ``query_mode='dict'`` (or set
        :py:attr:`.query_mode` in the constructor) to convert any row results into dictionaries which map
        each column to their values.
        
            >>> res, _, cur = db.query("SELECT * FROM users;", fetch='all', query_mode='dict')
        
        When querying with ``fetch='all'``, depending on your database API, ``res`` may be an iterator, and cannot
        be accessed via an index like ``res[0]``.
        
        You should make sure to iterate the rows using a ``for`` loop::
        
            >>> for row in res:
            ...     print(row['first_name'], ':', row)
            John : {'first_name': 'John', 'last_name': 'Doe', 'id': 12}
            Dave : {'first_name': 'Dave', 'last_name': 'Johnson', 'id': 13}
            Aaron : {'first_name': 'Aaron', 'last_name': 'Swartz', 'id': 14}
        
        Or, if the result object is a generator, then you can auto-iterate the results into a list
        using ``x = list(res)``::
            
            >>> rows = list(res)
            >>> rows[0]
            {'first_name': 'John', 'last_name': 'Doe', 'id': 12}
        
        Using the returned cursor (third return item), we can access various metadata about our query. Note that
        cursor objects vary between database APIs, and not all methods/attributes may be available, or may
        return different data than shown below::
            
            >>> cur.description  # cursor.description often contains the column names matching the query columns
            (('id', None, None, None, None, None, None), ('first_name', None, None, None, None, None, None),
             ('last_name', None, None, None, None, None, None))
            
            >>> _, _, cur = db.query("INSERT INTO users (first_name, last_name) VALUES ('a', 'b')", fetch='no')
            >>> cur.rowcount   # cursor.rowcount tells us how many rows were affected by a query
            1
            >>> cur.lastrowid  # cursor.lastrowid tells us the ID of the last row we inserted with this cursor
            3
            
        

        
        :param str sql: An SQL query to execute
        :param params: Any positional arguments other than ``sql`` will be passed to ``cursor.execute``.
        :param str fetch: Fetch mode. Either ``all`` (return ``cursor.fetchall()`` as first return arg),
                          ``one`` (return ``cursor.fetchone()``), or ``no`` (do not fetch. first return arg is None).
        :param kwparams: Any keyword arguments that aren't specified as parameters / keyword args for this method
                         will be forwarded to ``cursor.execute``
        
        :key GenericCursor cursor: Use this specific cursor instead of automatically obtaining one
        :key cursor_name: If your database API supports named cursors (e.g. PostgreSQL), then you may
                          specify ``cursor_name`` as a keyword argument to use a named cursor for this query.
        :key query_mode: Either ``flat`` (fetch results as they were originally returned from the DB), or
                         ``dict`` (use :meth:`._zip_cols` to convert tuple/list rows into dicts mapping col:value).
         
        :return iter results: (tuple item 1) An iterable such as a generator, or storage type e.g. ``list`` or ``dict``.
                              **NOTE:** If you've set ``fetch='all'``, depending on your database adapter, this
                              may be a generator or other form of iterator that cannot be directly accessed via index
                              (i.e. ``res[123]``). Instead you must iterate it with a ``for`` loop, or cast it into
                              a list/tuple to automatically iterate it into an indexed object, e.g. ``list(res)`
        
        :return Any res_exec: (tuple item 2) The object returned from running ``cur.execute(sql, *params, **kwparams)``.
                              This may be a cursor, but may also vary based on database API.
        
        :return GenericCursor cur: (tuple item 3) The cursor that was used to execute and fetch your query. To allow
                                   for use with server side cursors, the cursor is NOT closed automatically.
                                   To avoid stale cursors, it's best to run ``cur.close()`` when you're done with
                                   handling the returned results.
        
        """
        def _should_zip(_res):
            auto_zip = self.AUTO_ZIP_COLS
            is_dict_tuple = not isinstance(_res, dict) and not is_namedtuple(_res)
            return auto_zip and (is_dict_tuple and query_mode == 'dict')
            
        cursor_name = kwparams.pop('cursor_name', None)
        query_mode = kwparams.pop('query_mode', self.query_mode)
        c = kwparams.pop('cursor', self.get_cursor(cursor_name))
        res_exec = c.execute(sql, *params, **kwparams)
        if fetch == 'all':
            res = c.fetchall()
            if self.AUTO_ZIP_COLS and query_mode == 'dict':
                res = [self._zip_cols(c, r) for r in res]
        elif fetch == 'one':
            res = c.fetchone()
            if res is None:
                return None, res_exec, c
            if _should_zip(res):
                res = self._zip_cols(c, tuple(res))
        elif fetch == 'no':
            res = None
        else:
            raise AttributeError("The parameter 'fetch' must be either 'all', 'one' or 'no'.")
        if self.enable_execution_log:
            self._execution_log += [DBExecution(sql, res, res_exec, c)]
        return res, res_exec, c
    
    def action(self, sql: str, *params, **kwparams) -> int:
        """
        Use :meth:`.action` as a simple alias method for running "action" queries which don't return results, only
        affected row counts.
        
        For example ``INSERT``, ``UPDATE``, ``CREATE`` etc. queries.
        
        This method calls :meth:`.query` with ``fetch='no'``, saves the row count into a variable, closes the cursor,
        then returns the affected row count as an integer.
        
            >>> db = GenericDBWrapper('SomeDB')
            >>> rows_affected = db.action("DELETE FROM users WHERE first_name LIKE 'John%';")
            >>> rows_affected
            4
         
        :param str sql: An SQL query to execute on the current DB, as a string.
        :param params: Extra arguments will be passed through to ``cursor.execute(sql, *params, **kwparams)``
        :param kwparams: Extra arguments will be passed through to ``cursor.execute(sql, *params, **kwparams)``
        :return int row_count: Number of rows affected
        """
        # _, _, cur = self.query(sql, *params, fetch='no', **kwparams)
        with self.cursor as cur:
            self.query(sql, *params, fetch='no', cursor=cur, **kwparams)
            row_count = int(cur.rowcount)
        # cur.close()
        return row_count
    
    def fetch(self, sql: str, *params, fetch='all', **kwparams) \
            -> Optional[Union[dict, tuple, List[dict], List[tuple]]]:
        """
        Similar to :py:meth:`.query` but only returns the fetch results, not the execution object nor cursor.

        Example Usage (default query mode)::
            >>> s = GenericDBWrapper()
            >>> user = s.fetch("SELECT * FROM users WHERE id = ?;", [123], fetch='one')
            >>> user
            (123, 'john', 'doe',)

        Example Usage (dict query mode)::

            >>> s.query_mode = 'dict'    # Or s = SqliteWrapper(query_mode='dict')
            >>> res = s.fetch("SELECT * FROM users WHERE id = ?;", [123], fetch='one')
            >>> res
            {'id': 123, 'first_name': 'john', 'last_name': 'doe'}


        :param str fetch: Either ``'all'`` or ``'one'`` - controls whether the result is fetched with
                          :meth:`GenericCursor.fetchall` or :meth:`GenericCursor.fetchone`
        :param str sql: An SQL query to execute on the current DB, as a string.
        :param params: Extra arguments will be passed through to ``cursor.execute(sql, *params, **kwparams)``
        :param kwparams: Extra arguments will be passed through to ``cursor.execute(sql, *params, **kwparams)``
        :return:
        """
        res, _, cur = self.query(sql, *params, fetch=fetch, **kwparams)
        # query_mode = kwparams.pop('query_mode', self.query_mode)
        # if fetch == 'one':
        #     if res is None:
        #         return None
        # if query_mode == 'dict':
        # res = DictObject(res)
        cur.close()
        return res

    def fetchone(self, sql: str, *params, **kwparams) -> Optional[Union[dict, tuple]]:
        """Alias for :meth:`.fetch` with ``fetch='one'``"""
        return self.fetch(sql, *params, fetch='one', **kwparams)

    def fetchall(self, sql: str, *params, **kwparams) -> Optional[Union[List[dict], List[tuple]]]:
        """Alias for :meth:`.fetch` with ``fetch='all'``"""
        return self.fetch(sql, *params, fetch='all', **kwparams)

    @staticmethod
    def _zip_cols(cursor: GenericCursor, row: iter) -> DictObject:
        """Combine column names from ``cursor`` with the row values ``row`` into a singular dict ``res``"""
        # If the passed row is already a dictionary, just make sure the row is casted to a real dict and return it.
        if isinstance(row, dict):
            return DictObject(row)
        # combine the column names with the row data to create a dictionary of column_name:value
        col_names = list(map(lambda x: x[0], cursor.description))
        res = DictObject(zip(col_names, row))
        return res
    
    def get_cursor(self, cursor_name=None, cursor_class=None, *args, **kwargs) -> Union[CursorManager, GenericCursor]:
        """
        Create and return a new database cursor object, by default the cursor will be wrapped with
        :class:`.CursorManager` to ensure context management (``with`` statements) works regardless of whether
        the database API supports context managing cursors (e.g. :py:mod:`sqlite` does not support cursor contexts).
        
        For sub-classes, you should override :meth:`._get_cursor`, which returns an actual native DB cursor.
        
        :param str cursor_name:   (If DB API supports it) The name for this cursor
        :param type cursor_class: (If DB API supports it) The cursor class to use
        :key bool cursor_mgr: (Default: ``True``) If True, wrap the returned cursor with :class:`.CursorManager`
        :key callable close_callback: (Default: ``None``) Passed onto :class:`.CursorManager`
        :return GenericCursor cursor: A cursor object which should implement at least the basic Python DB API Cursor
                                      functionality as specified in :class:`.GenericCursor` ((PEP 249)
        """
        cursor_mgr = kwargs.pop('cursor_mgr', True)
        close_callback = kwargs.pop('close_callback', None)
        c = self._get_cursor(cursor_name=cursor_name, cursor_class=cursor_class, *args, **kwargs)
        self._cursors += [c]
        return CursorManager(c, close_callback=close_callback) if cursor_mgr else c

    @abc.abstractmethod
    def _get_cursor(self, cursor_name=None, cursor_class=None, *args, **kwargs):
        """
        Create and return a new database cursor object.

        It's recommended to override this method if you're inheriting from this class, as this Generic version of
        ``_get_cursor`` does not make use of ``cursor_name`` nor ``cursor_class``.

        :param str cursor_name:   (If DB API supports it) The name for this cursor
        :param type cursor_class: (If DB API supports it) The cursor class to use
        :return GenericCursor cursor: A cursor object which should implement at least the basic Python DB API Cursor
                                      functionality as specified in :class:`.GenericCursor` ((PEP 249)
        """
        return self.conn.cursor(*args, **kwargs)

    def _close_callback(self, state=None):
        log.debug("%s._close_callback was called with state: %s", self.__class__.__name__, state)
        if state == 'AFTER_CLOSE':
            self._cursor = None

    @property
    def cursor(self) -> GenericCursor:
        if self._cursor is None:
            # self._cursor = CursorManager(self.get_cursor(), close_callback=self._close_callback)
            self._cursor = self.get_cursor(cursor_mgr=True, close_callback=self._close_callback)
        return self._cursor

    def close_cursor(self):
        if self._cursor is None:
            return
        try:
            self._cursor.close()
        except (BaseException, Exception):
            log.exception("close_cursor was called but exception was raised...")
        self._cursor = None

    def create_schemas(self, *tables) -> dict:
        """
        Create all tables listed in :py:attr:`.SCHEMAS` if they don't already exist.

        :param str tables: If one or more table names are specified, then only these tables will be affected.
        :return dict result: ``dict_keys(['executions', 'created', 'skipped', 'tables_created', 'tables_skipped'])``
        """
        results = dict(executions=[], created=0, skipped=0, tables_created=[], tables_skipped=[])
        tb_count = len(self.SCHEMAS)
        tb_exists = 0
        for table, schema in self.SCHEMAS:
            tb_key = f"{self.db}:{table}"
            if tb_key in self.tables_created:
                log.debug('Found key %s in table_created.', tb_key)
                tb_exists += 1
                results['tables_skipped'] += [table]
    
        if tb_exists >= tb_count:
            log.debug('According to %s.tables_created, all tables already exist. Not creating schemas.',
                      self.__class__.__name__)
            results['skipped'] = len(results['tables_skipped'])
            return results
    
        # c = self.get_cursor()
        for table, schema in self.SCHEMAS:
            if len(tables) > 0 and table not in tables:
                log.debug("Table '%s' not specified in argument '*tables'. Skipping.", table)
                continue
            r = self.create_schema(table=table, schema=schema)
            results['executions'] += r.get('executions', [])
            results['tables_created'] += r.get('tables_created', [])
            results['tables_skipped'] += r.get('tables_skipped', [])
        # c.close()
        results['created'] = len(results['tables_created'])
        results['skipped'] = len(results['tables_skipped'])
        return results

    def create_schema(self, table: str, schema: str = None):
        """
        Create the individual table ``table``, either uses the create statement ``schema``, or if ``schema`` is empty,
        then checks for a pre-existing CREATE statement for ``table`` in :py:attr:`.SCHEMAS`.
        
            >>> db = GenericDBWrapper('SomeDBName')
            >>> db.create_schema('users', 'CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(200));')
        
        :param table:
        :param schema:
        :return:
        """
        tb_key = f"{self.db}:{table}"
        results = dict(executions=[], tables_created=[], tables_skipped=[])
        if tb_key in self.tables_created:
            log.debug('Skipping check for table %s as table is in table_created.', table)
            return None
        cls_name = self.__class__.__name__
        if empty(schema):
            _schemas = dict(self.SCHEMAS)
            if table not in _schemas:
                raise AttributeError(
                    f"Missing schema - cannot create table {table}. "
                    f"Table does not exist in {cls_name}.SCHEMAS and schema param was empty."
                )
            schema = _schemas[table]
        
        if not self.table_exists(table):
            log.debug("Table %s didn't exist. Creating it.", table)
            # cur = self.get_cursor()
            with self.cursor as cur:
                results['executions'] += [cur.execute(schema)]
                results['tables_created'] += [table]
            # cur.close()
        else:
            log.debug("Table %s already exists. Not creating it.", table)
            results['tables_skipped'] += [table]
    
        self.tables_created.add(tb_key)
        return results

    def recreate_schemas(self, *tables) -> dict:
        """
        Drop all tables then re-create them.

        Shortcut for running :meth:`.drop_schemas` followed by :meth:`.create_schemas`.

        :param str tables: If one or more table names are specified, then only these tables will be affected.
        :return dict result: ``dict_keys(['tables_created', 'skipped_create', 'skipped_drop', 'tables_dropped'])``
        """
        res = dict(tables_created=[], tables_dropped=[], skipped_create=[], skipped_drop=[])
        log.debug("\n-------------------------\n")
        dr = self.drop_schemas(*tables)
        self.conn.commit()
        log.debug("\n-------------------------\n")
        cr = self.create_schemas(*tables)
        self.conn.commit()
        log.debug("\n-------------------------\n")
    
        res['tables_created'] += cr['tables_created']
        res['skipped_create'] += cr['tables_skipped']
    
        res['tables_dropped'] += dr['tables_dropped']
        res['skipped_drop'] += dr['tables_skipped']
    
        return res

    def drop_schemas(self, *tables) -> dict:
        """
        Drop all tables listed in :py:attr:`.SCHEMAS` if they exist.

        :param str tables: If one or more table names are specified, then only these tables will be affected.
        :return dict result: ``dict_keys(['executions', 'created', 'skipped', 'tables_dropped', 'tables_skipped'])``
        """
        reversed_schemas = list(self.SCHEMAS)
        reversed_schemas.reverse()
        tables_drop = [t for t, _ in reversed_schemas]
        cls_name = self.__class__.__name__
        
        if len(tables) > 0:
            _tables = list(tables)
            log.debug("Tables specified to drop_schemas. Dropping tables: %s", _tables)
            tables_drop = [t for t, _ in reversed_schemas if t in _tables]
            tables_drop += [t for t in _tables if t not in reversed_schemas]
    
        results = dict(executions=[], created=0, skipped=0, tables_dropped=[], tables_skipped=[])
        for table in tables_drop:
            tb_key = f"{self.db}:{table}"
            if self.table_exists(table):
                log.debug("Table %s exists. Dropping it.", table)
                was_dropped = self.drop_table(table)
                
                if was_dropped:
                    if self.enable_execution_log:
                        results['executions'] += [self._execution_log[-1]]
                    else:
                        log.debug('%s.enable_execution_log is False. Not logging execution of drop_table', cls_name)
                    results['tables_dropped'] += [table]
                else:
                    log.debug("%s.drop_table('%s') returned False... Table wasn't dropped?",
                              self.__class__.__name__, table)
                    results['tables_skipped'] += [table]
            else:
                log.debug("Table %s doesn't exist. Not dropping it.", table)
                results['tables_skipped'] += [table]
            if tb_key in self.tables_created:
                log.debug('Removing key "%s" from tables_created, as table %s no longer exists.', tb_key, table)
                self.tables_created.remove(tb_key)
        return results

    def drop_table(self, table: str) -> bool:
        """
        Drop the table ``table`` if it exists. If the table exists, it will be dropped and ``True`` will be returned.

        If the table doesn't exist (thus can't be dropped), ``False`` will be returned.
        """
        if not self.table_exists(table):
            return False
        q = self.action(f'DROP TABLE {table};')
        return True
    
    def drop_tables(self, *tables) -> List[Tuple[str, bool]]:
        """
        Drop one or more tables as positional arguments.
        
        Returns a list of tuples containing ``(table_name:str, was_dropped:bool,)``
        :param str tables: One or more table names to drop as positional args
        :return list drop_results: List of tuples containing ``(table_name:str, was_dropped:bool,)``
        """
        return [(table, self.drop_table(table),) for table in tables]

    @abc.abstractmethod
    def __enter__(self):
        """
        Create the database connection at the start of a *with* statement, e.g.::
        
            >>> with GenericDBWrapper('SomeDB') as db:
            ...     db.query("INSERT INTO users (name) VALUES ('Bob');", fetch='no')
            ...

        """
        self._conn = self.conn
        return self

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close / delete the database connection at the end of a *with* statement, e.g.::

            >>> with GenericDBWrapper('SomeDB') as db:
            ...     db.query("INSERT INTO users (name) VALUES ('Bob');", fetch='no')
            ...

        """
        self.close_cursor()
        if self._conn is not None:
            self._conn.close()
            del self._conn
        self._conn = None
