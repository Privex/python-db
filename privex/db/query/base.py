import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Iterable, Union, Optional

from privex.db.base import CursorManager
from privex.db.types import GenericCursor, GenericConnection

log = logging.getLogger(__name__)


class QueryMode(Enum):
    """
    A small :class:`enum.Enum` used for the ``query_mode`` (whether to return rows as tuples or dicts) with
    Query Builder classes (see :class:`.BaseQueryBuilder` :class:`.SqliteQueryBuilder` :class:`.PostgresQueryBuilder`)
    """
    DEFAULT = 'default'
    ROW_TUPLE = 'tuple'
    ROW_DICT = 'dict'


class BaseQueryBuilder(ABC):
    """
    This is an SQL query builder class which outputs ANSI compatible SQL queries, and can use connections/cursors
    to execute the queries that it builds.
    
    This is an **abstract base class** (:class:`abc.ABC`) meaning that it's not designed to be constructed directly,
    instead it should be used as a parent class for a database specific query builder, for example
    :class:`.SqliteQueryBuilder` or :class:`.PostgresQueryBuilder`.
    
    To implement a sub-class of :class:`.BaseQueryBuilder`, you must:
    
    * Implement all methods marked with ``@abstractmethod``, such as :meth:`.build_query`, :meth:`.all`
      and :meth:`.fetch`
    
    * If your DBMS or it's Python API doesn't follow the default query configuration (see the attributes
      starting with ``Q_``), then you should adjust the ``Q_`` attributes in your class to match your DB / DB API.
      
      e.g. Set ``Q_DEFAULT_PLACEHOLDER = "?"`` if your DB API expects ``?`` for prepared statement placeholders instead
      of ``%s``.
    
    * While not required, you may wish to implement a constructor (:meth:`.__init__`), and override :meth:`.get_cursor`
      to adjust it to your database API requirements
    
    """
    query: str
    table: str
    select_cols: List[str]
    group_cols: List[str]
    where_clauses: List[str]
    where_clauses_values: List[str]
    order_cols: List[str]
    order_dir: str
    order_dir: str
    
    connection = None
    _cursor: Optional[GenericCursor]
    _is_executed: bool

    # noinspection SqlNoDataSourceInspection
    Q_SELECT_CLAUSE = ' SELECT {cols} FROM {table}'
    Q_WHERE_CLAUSE = ' WHERE {w_clauses}'
    Q_LIMIT_CLAUSE = ' LIMIT {limit}'
    Q_OFFSET_CLAUSE = ' OFFSET {offset}'
    Q_ORDER_CLAUSE = ' ORDER BY {order_cols} {order_dir}'
    Q_GROUP_BY_CLAUSE = ' GROUP BY {group_cols}'
    Q_PRE_QUERY = ""
    Q_POST_QUERY = ""
    Q_DEFAULT_PLACEHOLDER = "%s"
    
    def __init__(self, table: str, connection: GenericConnection = None, **kwargs):
        self.query = ""
        self.connection = connection
        self.table = table
        self.select_cols = []
        self.group_cols = []
        self.where_clauses = []
        self.where_clauses_values = []
        self.order_cols = []
        self.order_dir = ''
        self.limit_num = None
        self.limit_offset = None
        self._cursor = None
        self._is_executed = False
    
    def _close_callback(self, state=None):
        log.debug("%s._close_callback was called with state: %s", self.__class__.__name__, state)
        if self._cursor is not None:
            self._cursor = None

    def get_cursor(self, cursor_name=None, cursor_class=None, *args, **kwargs):
        """
        Create and return a new database cursor object.

        It's recommended to override this method if you're inheriting from this class, as this Generic version of
        ``get_cursor`` does not make use of ``cursor_name`` nor ``cursor_class``.

        :param str cursor_name:   (If DB API supports it) The name for this cursor
        :param type cursor_class: (If DB API supports it) The cursor class to use
        :return GenericCursor cursor: A cursor object which should implement at least the basic Python DB API Cursor
                                      functionality as specified in :class:`.GenericCursor` ((PEP 249)
        """
        c = self.connection.cursor(*args, **kwargs)
        return c
    
    @property
    def cursor(self) -> GenericCursor:
        if self._cursor is None:
            self._cursor = CursorManager(self.get_cursor(), close_callback=self._close_callback)
        return self._cursor
    
    def close_cursor(self):
        if not hasattr(self, '_cursor') or self._cursor is None:
            return
        try:
            self._cursor.close()
        except (BaseException, Exception):
            log.exception("close_cursor was called but exception was raised...")
        try:
            self._cursor = None
        except:
            pass
    
    @abstractmethod
    def build_query(self) -> str:
        """
        Used internally by :py:meth:`.all` and :py:meth:`.fetch` - builds and returns a string SQL query using the
        various class attributes such as :py:attr:`.where_clauses`
        :return str query: The SQL query that will be sent to the database as a string
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement .build_query()!")

    def _build_query(self) -> str:
        """
        This is a stock :meth:`.build_query` method which can be used by sub-classes if their DBMS is compatible
        with the ANSI SQL outputted by this method.
        
        Example::
        
            >>> class SomeDBQueryBuilder(BaseQueryBuilder):
            >>>     def build_query(self) -> str:
            ...         return super()._build_query()
        
        
        :return str query: The SQL query that will be sent to the database as a string
        """
        s_cols = ', '.join(self.select_cols) if len(self.select_cols) > 0 else '*'
        # q = f"{self.Q_PRE_QUERY} "
        q = self.Q_PRE_QUERY
        # SELECT {s_cols} FROM {self.table}
        q += self.Q_SELECT_CLAUSE.format(cols=s_cols, table=self.table)
        if len(self.where_clauses) > 0:
            w_clauses = ' '.join(self.where_clauses)
            # q += f" WHERE {w_clauses}"
            q += self.Q_WHERE_CLAUSE.format(w_clauses=w_clauses)
        if len(self.group_cols) > 0:
            g_cols = ', '.join(self.group_cols)
            # q += f" GROUP BY {g_cols}"
            q += self.Q_GROUP_BY_CLAUSE.format(group_cols=g_cols)
        if len(self.order_cols) > 0:
            # q += f" ORDER BY {', '.join(self.order_cols)} {self.order_dir}"
            q += self.Q_ORDER_CLAUSE.format(order_cols=', '.join(self.order_cols), order_dir=self.order_dir)
        if self.limit_num is not None:
            # q += f" LIMIT {self.limit_num}"
            q += self.Q_LIMIT_CLAUSE.format(limit=self.limit_num)
            if self.limit_offset is not None:
                q += self.Q_OFFSET_CLAUSE.format(offset=self.limit_offset)
                # q += f" OFFSET {self.limit_offset}"
        
        q += ';'
        
        log.debug(f"Built query: '''\n{q}\n'''")
    
        return q
    
    def execute(self, *args, **kwargs):
        _exec = self.cursor.execute(self.build_query(), self.where_clauses_values, *args, **kwargs)
        self._is_executed = True
        return _exec
    
    @abstractmethod
    def all(self, query_mode=QueryMode.ROW_DICT) -> Union[Iterable[dict], Iterable[tuple]]:
        """
        Executes the current query, and returns an iterable cursor (results are loaded as you iterate the cursor)

        Usage:

        >>> results = BaseQueryBuilder('people').all()   # Equivalent to ``SELECT * FROM people;``
        >>> for r in results:
        >>>     print(r['first_name'], r['last_name'], r['phone'])

        :return Iterable: A cursor which can be iterated using a ``for`` loop.
                          Ideally, should load rows as you iterate, saving RAM.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement .all()!")

    @abstractmethod
    def fetch(self, query_mode=QueryMode.ROW_DICT) -> Union[dict, tuple, None]:
        """
        Executes the current query, and fetches the first result as a ``dict``.

        If there are no results, will return None

        :return dict: The query result as a dictionary: {column: value, }
        :return None: If no results are found
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement .fetch()!")

    @abstractmethod
    def fetch_next(self, query_mode=QueryMode.ROW_DICT) -> Union[dict, tuple, None]:
        """
        Similar to :meth:`.fetch`, but doesn't close the cursor after the query, so can be ran more than once
        to iterate row-by-row over the results.
        
        :param QueryMode query_mode:
        :return:
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement .fetch_next()!")

    def select(self, *args):
        """
        Add columns to select clause, specify as individual args. NOTE: no escaping!

        example:

        q.select('mycol', 'othercol', 'somecol as thiscol')

        can also chain: q.select('mycol').select('othercol')

        :param args: columns to select as individual arguments
        :return: QueryBuilder object (for chaining)
        """
        self.select_cols += list(args)
        return self

    def order(self, *args, direction='DESC'):
        """
        example: order('mycol', 'othercol') == ORDER BY mycol, othercol DESC

        :param args: One or more order columns as individual args
        :param direction: Direction to sort
        :return: QueryBuilder object (for chaining)
        """
        self.order_cols = list(args)
        self.order_dir = direction
        return self
    
    def order_by(self, *args, **kwargs):
        """Alias of :meth:`.order`"""
        return self.order(*args, **kwargs)

    def where(self, col, val, compare='=', placeholder=None):
        """
        For adding a simple col=value clause with "AND" before it (if at least 1 other clause). val is escaped properly

        example: where('x','test').where('y','thing') produces prepared sql "WHERE x = %s AND y = %s"

        :param col: the column, function etc. to query
        :param val: the value it should be equal to. most python objects will be converted and escaped properly
        :param compare: instead of '=', compare using this comparator, e.g. '>', '<=' etc.
        :param placeholder: Set the value placeholder, e.g. placeholder='HOST(%s)'
        :return: QueryBuilder object (for chaining)
        """
        placeholder = self.Q_DEFAULT_PLACEHOLDER if placeholder is None else placeholder
        
        # Convert .where('name', None) into "name IS NULL"
        if val is None:
            placeholder = 'NULL'
            if compare == '=': compare = 'IS'
            elif compare == '!=': compare = 'IS NOT'
        else:
            self.where_clauses_values += [val]
        
        if len(self.where_clauses) > 0:
            self.where_clauses += ['AND {} {} {}'.format(col, compare, placeholder)]
            return self
        self.where_clauses += ['{} {} {}'.format(col, compare, placeholder)]
        return self

    def where_or(self, col, val, compare='=', placeholder=None):
        """
        For adding simple col=value clause with "OR" before it (if at least 1 other clause). val is escaped properly

        example: where('x','test').where_or('y','thing') produces prepared sql "WHERE x = %s OR y = %s"

        :param col: the column, function etc. to query
        :param val: the value it should be equal to. most python objects will be converted and escaped properly
        :param compare: instead of '=', compare using this comparator, e.g. '>', '<=' etc.
        :param placeholder: Set the value placeholder, e.g. placeholder='HOST(%s)'
        :return: QueryBuilder object (for chaining)
        """
        placeholder = self.Q_DEFAULT_PLACEHOLDER if placeholder is None else placeholder

        self.where_clauses_values += [val]
    
        if len(self.where_clauses) > 0:
            self.where_clauses += ['OR {} {} {}'.format(col, compare, placeholder)]
            return self
        self.where_clauses += ['{} {} {}'.format(col, compare, placeholder)]
        return self

    def limit(self, limit_num, offset=None):
        """
        Add a limit/offset. When using offset you should use an ORDER BY to avoid issues.
        :param limit_num: Amount of rows to limit to
        :param offset: Offset by this many rows (optional)
        :return: QueryBuilder object (for chaining)
        """
        self.limit_num = limit_num
        if offset is not None:
            self.limit_offset = offset
    
        return self

    def group_by(self, *args):
        """
        Add one or more columns to group by clause.

        example: group_by('name', 'date') == GROUP BY name, date

        :param args: One or more columns to group by
        :return: QueryBuilder object (for chaining)
        """
        self.group_cols += list(args)
        return self
    
    def __iter__(self):
        """
        Allow the query object to be iterated over to get results.
        
        Iterating over the query builder object is equivalent to iterating over :meth:`.all`
        
            >>> q = BaseQueryBuilder('users')
            >>> for r in q.select('username', 'first_name').where('id', 10, '>='):
            ...     print(r['username'], r['first_name'])
        
        """
        for r in self.all():
            yield r
    
    def __next__(self):
        return self.fetch_next()
    
    def __getitem__(self, item):
        if type(item) is int:
            if item == 0:
                return self.fetch(query_mode=QueryMode.ROW_DICT)
            return list(self.all())[item]
        if type(item) is str:
            return self.fetch(query_mode=QueryMode.ROW_DICT)[item]

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_cursor()
    
    def __del__(self):
        self.close_cursor()
    

