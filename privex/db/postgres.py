"""
This module holds :class:`.PostgresWrapper` - a somewhat higher level class for interacting with PostgreSQL.


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
import logging
from typing import List, Tuple, Optional, Any, Union, Set, Dict

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import psycopg2
from privex.helpers import DictObject

from privex.db.query import PostgresQueryBuilder
from privex.db.base import GenericDBWrapper

log = logging.getLogger(__name__)

PG_Cursor = psycopg2.extensions.cursor

psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)


class PostgresWrapper(GenericDBWrapper):
    """
    Lightweight wrapper class for interacting with PostgreSQL databases.

    **Usage**

    .. code-block:: python

        class MyManager(PostgresWrapper):
            SCHEMAS: List[Tuple[str, str]] = [
                ('users', "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50));"),
                ('items', "CREATE TABLE items (id INTEGER PRIMARY KEY, name VARCHAR(50));"),
            ]

            def get_items(self):
                return self.fetchall("SELECT * FROM items;")

            def find_item(self, id: int):
                return self.fetchone("SELECT * FROM items WHERE id = %s;", [id]);



    """
    
    DEFAULT_DB: str = None
    DEFAULT_QUERY_MODE = 'dict'
    DEFAULT_PLACEHOLDER = '%s'
    
    DEFAULT_TABLE_QUERY = """
    SELECT EXISTS (
       SELECT 1
       FROM   pg_catalog.pg_class c
       JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
       WHERE  n.nspname = %s
       AND    c.relname = %s
       -- only tables
       AND    c.relkind = 'r'
   );
    """
    DEFAULT_TABLE_LIST_QUERY = """
    SELECT relname as name
       FROM   pg_catalog.pg_class c
       JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
       WHERE  n.nspname = %s
       -- only tables
       AND    c.relkind = 'r';
    """

    AUTO_ZIP_COLS = False
    
    db: str
    """PostgreSQL database name"""
    
    _cursor_cls: psycopg2.extensions.cursor
    
    cursor_map: Dict[str, PG_Cursor] = {
        'flat': psycopg2.extras.NamedTupleCursor,
        'dict': psycopg2.extras.RealDictCursor,
    }
    
    _conn: Optional[psycopg2.extensions.connection]
    """Instance variable which holds the current Postgres connection object"""
    
    _builder: Optional[PostgresQueryBuilder]
    
    def __init__(self, db=None, db_user='root', db_host=None, db_pass=None, **kwargs):
        """
        Initialise the database wrapper class.
        
        
        :param str db: Database name
        :param str db_user: Account username with permission for ``db`` (defaults to ``root``)
        :param str db_pass: Account password for ``db_user`` (defaults to ``None``)
        :param str db_host: Database host (defaults to unix socket)

        :key str db_schema: (Default: ``'public'``) Schema used for querying table existence
        :key str query_mode: Either ``'flat'`` (query returns tuples) or ``'dict'`` (query returns dicts).
                             More details in PyDoc block under :py:attr:`.query_mode`
        :key psycopg2.extensions.cursor cursor_cls: If necessary, you may override the Psycopg2 cursor class used
                    by specifying this kwarg. If this isn't specified, :py:attr:`.cursor_cls` will default to either
                    :class:`psycopg2.extras.RealDictCursor` if query_mode is ``dict``, or
                    :class:`psycopg2.extras.NamedTupleCursor` if query_mode is ``flat``.
                    
        """
        db = self.DEFAULT_DB if db is None else db
        self.db = db
        self.db_schema = kwargs.pop('db_schema', 'public')
        self.isolation_level = kwargs.pop('isolation_level', ISOLATION_LEVEL_AUTOCOMMIT)
        self.query_mode = kwargs.pop('query_mode', self.DEFAULT_QUERY_MODE)
        self._conn = None
        self._builder = None
        # self.cursor_cls = psycopg2.extras.NamedTupleCursor
        auto_create_schema = kwargs.pop('auto_create_schema', True)
        
        self._cursor_cls = kwargs.get('cursor_cls', None)
        # elif self.query_mode == 'dict':
        #     self.cursor_cls = psycopg2.extras.RealDictCursor
        # elif self.query_mode == 'flat':
        #     self.cursor_cls = psycopg2.extras.NamedTupleCursor
        # else:
        if self.query_mode not in ['dict', 'flat']:
            raise AttributeError("query_mode must be one of 'dict' or 'flat'")

        super(PostgresWrapper, self).__init__(
            db=db, connector_func=psycopg2.connect, query_mode=self.query_mode,
            connector_kwargs=dict(dbname=db, user=db_user, password=db_pass, host=db_host),
            auto_create_schema=False, **kwargs
        )
        
        conn = self.conn
        conn: psycopg2.extensions.connection
        conn.reset()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        if auto_create_schema:
            res = self.create_schemas()
            log.debug('Create schema result: "%s"', res)
    
    @property
    def cursor_cls(self):
        if self._cursor_cls is not None:
            return self._cursor_cls
        elif self.query_mode == 'dict':
            return psycopg2.extras.RealDictCursor
        elif self.query_mode == 'flat':
            return psycopg2.extras.NamedTupleCursor
    
    def _get_cursor(self, cursor_name=None, cursor_class=None, *args, **kwargs):
        cursor_class = self.cursor_cls if cursor_class is None else cursor_class
        if cursor_name is not None:
            c = self.conn.cursor(cursor_name, cursor_factory=cursor_class)
        else:
            c = self.conn.cursor(cursor_factory=cursor_class)
        # self._cursors += [c]
        return c

    def query(self, sql: str, *params, fetch='all', **kwparams) -> Tuple[Optional[iter], Any, PG_Cursor]:
        args, kwargs = [sql] + list(params), {'fetch': fetch, **kwparams}
        # For a dynamic query_mode to work, we have to override the query cursor to use
        # either NamedTupleCursor (for 'flat') or RealDictCursor (for 'dict')
        if 'query_mode' in kwparams and 'cursor' not in kwparams:
            kwargs['cursor'] = self.get_cursor(cursor_class=self.cursor_map[kwparams['query_mode']])
        return super().query(*args, **kwargs)
    
    def insert(self, _table: str, _cursor: PG_Cursor = None, **fields) -> Union[DictObject, PG_Cursor]:
        return super().insert(_table, _cursor, **fields)

    def table_exists(self, table: str, schema: str = None) -> bool:
        schema = self.db_schema if schema is None else schema
        res = self.fetchone(self.table_query, [schema, table])
        if isinstance(res, dict):
            log.debug("Table %s exists: %s", table, res['exists'])
            return res['exists']
        else:
            log.debug("Table %s exists: %s", table, res[0])
            return res[0]

    def list_tables(self, schema: str = None) -> List[str]:
        """
        Get a list of tables present in the current database.

        Example::

            >>> GenericDBWrapper().list_tables()
            ['sqlite_sequence', 'nodes', 'node_api', 'node_failures']


        :return List[str] tables: A list of tables in the database
        """
        schema = self.db_schema if schema is None else schema

        res = self.fetchall(self.table_list_query, [schema])
        if len(res) < 1:
            return []
        if type(res[0]) is dict or isinstance(res[0], psycopg2.extras.RealDictRow):
            return [r['name'] for r in res]
        else:
            return [r[0] for r in res]
    
    # noinspection PyTypeChecker
    @property
    def conn(self) -> psycopg2.extensions.connection:
        """Get or create a Postgres connection"""
        conn = super().conn
        conn: psycopg2.extensions.connection
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    @staticmethod
    def _zip_cols(cursor: psycopg2.extensions.cursor, row: iter):
        """
        Combine column names from the cursor ``cursor`` with the row data ``row`` to convert the result row into
        a dictionary, mapping column names to values.
        
        We override this from :class:`.GenericDBWrapper`, as Postgres cursor description contains ``Column``
        objects, instead of plain tuples.
        """
        # If the passed row is already a dictionary, or a postgres RealDictRow, just make sure the row is casted to
        # a real dict and return it back.
        if isinstance(row, dict) or isinstance(row, psycopg2.extras.RealDictRow):
            return DictObject(row)
        # Otherwise, extract the columns
        col_names = list(map(lambda x: x.name, cursor.description))
        res = DictObject(zip(col_names, row))
        return res

    def drop_table(self, table: str) -> bool:
        """
        Drop the table ``table`` if it exists. If the table exists, it will be dropped and ``True`` will be returned.

        If the table doesn't exist (thus can't be dropped), ``False`` will be returned.
        """
        if not self.table_exists(table):
            return False
        q = self.query(f'DROP TABLE IF EXISTS {table};', fetch='no')
        q[2].close()
        return True
    
    def builder(self, table: str) -> PostgresQueryBuilder:
        return PostgresQueryBuilder(table=table, connection=self.conn)

    def last_insert_id(self, table_name: str, pk_name='id'):
        """
        Get the last inserted ID for a given table + primary key.
        
        Example::
            
            >>> db = PostgresWrapper(db='my_db')
            >>> db.action('INSERT INTO users (first_name, last_name) VALUES (?, ?);', ['John', 'Doe'])
            >>> last_id = db.last_insert_id('users')
            >>> db.fetchone('SELECT first_name, last_name FROM users WHERE id = ?', [last_id])
            Record(id=16, first_name='John', last_name='Doe')
            
        :param str table_name: The table you want the last insertion for
        :param str pk_name: The primary key name, e.g. ``id``, ``username`` etc.
        :return Any last_id: The last ``pk_name`` inserted into ``table_name``
        """
        sequence = f"{table_name}_{pk_name}_seq"
        res = self.fetchone(f"SELECT last_value from {sequence}", query_mode='flat')
        return res[0]
    
    def __enter__(self):
        self._conn = self.conn
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            self._conn.close()
        self._conn = None
