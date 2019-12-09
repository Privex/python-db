import sqlite3
from typing import Union, Coroutine, Any, AsyncIterable

import aiosqlite
from aiosqlite import Cursor
from async_property import async_property
from privex.helpers import DictObject

from privex.db.query.asyncx.base import BaseAsyncQueryBuilder
from privex.db.query.base import QueryMode
from privex.db.types import GenericCursor, GenericAsyncCursor, TUP_DICT, TUPDICT_OPT, GenericAsyncConnection


def _zip_cols(cursor: Union[sqlite3.Cursor, GenericCursor, GenericAsyncCursor], row: iter):
    # combine the column names with the row data
    # so it can be used like a dict
    col_names = list(map(lambda x: x[0], cursor.description))
    res = DictObject(zip(col_names, row))
    return res


class SqliteAsyncQueryBuilder(BaseAsyncQueryBuilder):
    Q_DEFAULT_PLACEHOLDER = '?'
    Q_PRE_QUERY = ''
    _connection: aiosqlite.Connection
    _cursor = Coroutine[Any, Any, Cursor]
    
    def __init__(self, table: str, connection_args: list = None, connection_kwargs: dict = None, **kwargs):
        super().__init__(table, connection_args=connection_args, connection_kwargs=connection_kwargs, **kwargs)
        self._connection = None

    @async_property
    async def connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            self._connection = await self.get_connection()
        return self._connection

    async def get_connection(self) -> aiosqlite.Connection:
        return aiosqlite.connect(*self.connection_args, **self.connection_kwargs)
    
    # @async_property
    # async def cursor(self) -> Cursor:
    #     if self._cursor is None:
    #         # conn = self.connection
    #         # if asyncio.iscoroutine(conn):
    #         conn = await self.connection
    #         await conn._connect()
    #         self._cursor = await conn.cursor()
    #     return self._cursor
    
    # async def _get_cursor(self, cursor_name=None, cursor_class=None, *args, **kwargs) -> aiosqlite.Connection:
    #     return self.conn

    async def _build_query(self) -> str:
        return await super()._build_query()

    async def execute(self, *args, **kwargs) -> Any:
        _cur = kwargs.pop('cursor', None)
        if _cur is None:
            if self._connection is None:
                _cur = self._connection = await self.get_connection()
            else:
                _cur = self._connection
                
        # conn = self._connection = await self.get_connection()
        # conn = await self.connection
        
        self._cursor = await _cur.execute(await self.build_query(), self.where_clauses_values)
        self._is_executed = True
        return self._cursor

    async def all(self, query_mode=QueryMode.ROW_DICT) -> AsyncIterable[Union[tuple, dict]]:
        # await self.execute()
        async def _all_body(connection):
            cur = await self.execute(cursor=connection)
            for res in await cur.fetchall():
                if query_mode == QueryMode.ROW_DICT:
                    yield _zip_cols(cur, res)
                else:
                    yield res
        
        if self._connection is None:
            _conn = await self.get_connection()
            async with _conn as conn:
                async for row in _all_body(conn):
                    yield row
        else:
            _conn = self._connection
            async for row in _all_body(_conn):
                yield row
        
        # async with _conn as conn:
        #     cur = await self.execute(cursor=conn)
        #     for res in await cur.fetchall():
        #         if query_mode == QueryMode.ROW_DICT:
        #             yield _zip_cols(cur, res)
        #         else:
        #             yield res
    
        self._cursor = None
    
    async def fetch(self, query_mode=QueryMode.ROW_DICT) -> TUPDICT_OPT:
        async def _fetch_body(connection):
            cur = await self.execute(cursor=connection)
            res = await cur.fetchone()
            if len(res) > 0 and query_mode == QueryMode.ROW_DICT:
                res = _zip_cols(cur, tuple(res))
            return res
        
        # with self.cursor as cur:
        if self._connection is None:
            _conn = await self.get_connection()
            async with _conn as conn:
                return await _fetch_body(conn)
                # cur = await self.execute(cursor=conn)
                # res = await cur.fetchone()
                # if len(res) > 0 and query_mode == QueryMode.ROW_DICT:
                #     res = _zip_cols(cur, tuple(res))
                # return res
        
        return await _fetch_body(self._connection)
        # self._cursor = None
        # return res

    async def fetch_next(self, query_mode=QueryMode.ROW_DICT) -> TUPDICT_OPT:
        if self._connection is None:
            name = self.__class__.__name__
            raise ConnectionError(
                f"To use {name}.fetch_next() you MUST use this class in an async context manager, e.g: \n"
                f"\tasync with {name}('users', connection_args=['example.db']) as b:\n"
                f"\t\tuser = await b.__anext__()\n"
            )
        if not self._is_executed:
            await self.execute()
        res = await self._cursor.fetchone()
        if len(res) > 0 and query_mode == QueryMode.ROW_DICT:
            res = _zip_cols(self._cursor, tuple(res))
        if res is None or len(res) == 0:
            self._cursor = None
        return res
    
    async def __aenter__(self):
        self._connection = await self.get_connection()
        await self._connection.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._connection.__aexit__(None, None, None)

