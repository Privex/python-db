from typing import Iterable, Union
import psycopg2.extras
import psycopg2.extensions
import logging

from privex.db.base import CursorManager
from privex.db.types import GenericCursor
from privex.db.query.base import BaseQueryBuilder, QueryMode

log = logging.getLogger(__name__)


class PostgresQueryBuilder(BaseQueryBuilder):
    """
    A simple SQL query builder / ORM, designed for use with PostgreSQL. May or may not work with other RDBMS's.

    Basic Usage:

        First, inject your psycopg2 connection into QueryBuilder, so it's available to all instances.

        >>> PostgresQueryBuilder.conn = psycopg2.connect(user='bob', dbname='my_db')

        Now, just construct the class, passing the table name to query.

        >>> q = PostgresQueryBuilder('orders')

        You can execute each query building method either on their own line, and/or you can chain them together.

        **WARNING:** many methods such as :py:meth:`.select` do not escape your input. Only :py:meth:`.where` and
        :py:meth:`.where_or` use prepared statements, with a placeholder for the value you pass.

        >>> q.select('full_name', 'address')
        >>> q.select('SUM(order_amt) as total_spend').where('country', 'FR') \
        ...     .where('SUM(order_amt)', '100', compare='>=')
        >>> q.group_by('full_name', 'address')

        Once you've finished building your query, simply call either :py:meth:`.all` (return all results as a list)
        or :py:meth:`.fetch` (returns the first result, or ``None`` if there's no match)

        >>> results = q.order('full_name', direction='ASC').all()
        >>> print(results[0])

        Output::

            dict{'full_name': 'Aaron Doe', 'address': '123 Fake St.', 'total_spend': 127.88}


        You can call :py:meth:`.build_query` to see the query that would be sent to PostgreSQL, showing the
        value placeholders (e.g. %s)

        >>> print(q.build_query())

        Output::

            SELECT full_name, address, SUM(order_amt) as total_spend FROM orders WHERE country = %s
            AND SUM(order_amt) >= %s GROUP BY full_name, address ORDER BY full_name ASC;


    Copyright::

        +===================================================+
        |                 © 2019 Privex Inc.                |
        |               https://www.privex.io               |
        +===================================================+
        |                                                   |
        |        Privex Database Library                    |
        |                                                   |
        |        Core Developer(s):                         |
        |                                                   |
        |          (+)  Chris (@someguy123) [Privex]        |
        |                                                   |
        +===================================================+


    """

    Q_PRE_QUERY = "set timezone to 'UTC'; "
    Q_DEFAULT_PLACEHOLDER = "%s"
    
    cursor_cls: psycopg2.extensions.cursor
    query_mode: QueryMode
    
    @property
    def conn(self) -> psycopg2.extensions.connection:
        return self.connection

    def __init__(self, table: str, connection=None, **kwargs):
        super().__init__(table, connection)
        
        self.query_mode = query_mode = kwargs.pop('query_mode', QueryMode.ROW_DICT)
        if query_mode == QueryMode.ROW_DICT:
            cursor_cls = psycopg2.extras.RealDictCursor
        elif query_mode == QueryMode.ROW_TUPLE:
            cursor_cls = psycopg2.extras.NamedTupleCursor
        else:
            raise AttributeError('query_mode must be one of QueryMode.ROW_DICT or ROW_TUPLE')
        self.cursor_cls = kwargs.pop('cursor_cls', cursor_cls)
        self._cursor_map = {
            QueryMode.DEFAULT: self.cursor_cls,
            QueryMode.ROW_DICT: psycopg2.extras.RealDictCursor,
            QueryMode.ROW_TUPLE: psycopg2.extras.NamedTupleCursor
        }

    def fetch_next(self, query_mode=QueryMode.ROW_DICT) -> Union[dict, tuple, None]:
        if not self._is_executed:
            self.execute()
        return self.cursor.fetchone()
    
    def query_mode_cursor(self, query_mode: QueryMode, replace_cursor=True, cursor_mgr=True):
        """
        Return a cursor object with the cursor class based on the ``query_mode``, using the
        query_mode to cursor class map in :py:attr:`._cursor_map`
        
        :param QueryMode query_mode: The QueryMode to obtain a cursor for
        :param bool replace_cursor: (Default: ``True``) If True, replace the shared instance :py:attr:`._cursor` with
                                    this new cursor.
        :param bool cursor_mgr: Wrap the cursor object in :class:`.CursorManager`
        :return:
        """
        _cur = self.get_cursor(cursor_class=self._cursor_map[query_mode])
        if cursor_mgr:
            _cur = CursorManager(_cur, close_callback=self._close_callback)
        if replace_cursor:
            try:
                self.close_cursor()
            except (BaseException, Exception):
                pass
            self._cursor = _cur
        return _cur
    
    def build_query(self) -> str:
        return self._build_query()

    def get_cursor(self, cursor_name=None, cursor_class=None, *args, **kwargs) -> psycopg2.extensions.cursor:
        """Create and return a new Postgres cursor object"""
        cur_cls = self.cursor_cls if cursor_class is None else cursor_class
        if cursor_name is not None:
            return self.conn.cursor(cursor_name, cursor_factory=cur_cls)
        else:
            return self.conn.cursor(cursor_factory=cur_cls)

    @property
    def cursor(self) -> psycopg2.extensions.cursor:
        if self._cursor is None:
            _cur = self.conn.cursor(cursor_factory=self.cursor_cls)
            self._cursor = CursorManager(_cur, close_callback=self._close_callback)
        return self._cursor
    
    def all(self, query_mode=QueryMode.DEFAULT) -> Union[Iterable[dict], Iterable[tuple]]:
        """
        Executes the current query, and returns an iterable cursor (results are loaded as you iterate the cursor)

        Usage:

        >>> results = PostgresQueryBuilder('people').all()   # Equivalent to ``SELECT * FROM people;``
        >>> for r in results:
        >>>     print(r['first_name'], r['last_name'], r['phone'])

        :return Iterable: A cursor which can be iterated using a ``for`` loop, loads rows as you iterate, saving RAM
        """
        if self.conn is None:
            raise Exception('Please statically set PostgresQueryBuilder.conn to a psycopg2 connection')
        
        # if query_mode == QueryMode.DEFAULT: cursor_cls = self.cursor_cls
        # elif query_mode == QueryMode.ROW_DICT: cursor_cls = psycopg2.extras.RealDictCursor
        # elif query_mode == QueryMode.ROW_TUPLE: cursor_cls = psycopg2.extras.NamedTupleCursor
        if query_mode not in self._cursor_map:
            raise AttributeError('query_mode must be one of QueryMode.ROW_DICT or ROW_TUPLE')
        
        with self.query_mode_cursor(query_mode, False) as cur:
            cur.execute(self.build_query(), self.where_clauses_values)
            return cur.fetchall()

    def fetch(self, query_mode=QueryMode.DEFAULT) -> Union[dict, tuple, None]:
        """
        Executes the current query, and fetches the first result as a ``dict``.

        If there are no results, will return None

        :return dict: The query result as a dictionary: {column: value, }
        :return None: If no results are found
        """
        if self.conn is None:
            raise Exception('Please statically set PostgresQueryBuilder.conn to a psycopg2 connection')
        if query_mode not in self._cursor_map:
            raise AttributeError('query_mode must be one of QueryMode.ROW_DICT or ROW_TUPLE')
        with self.query_mode_cursor(query_mode, False) as cur:
            cur.execute(self.build_query(), self.where_clauses_values)
            return cur.fetchone()
    
    def select_date(self, *args):
        """
        Add columns to be returned as an ISO formatted date to the select clause.
        Specify as individual args. Do not use 'col AS x'. NOTE: no escaping is used!

        example: q.select_date('created_at', 'updated_at')
        can also chain: q.select_date('mycol').select_date('othercol')

        :param args: date columns to select as individual arguments
        :return: QueryBuilder object (for chaining)
        """
        self.select_cols += ["""to_char({a}, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as {a}""".format(a=a) for a in args]
        return self

