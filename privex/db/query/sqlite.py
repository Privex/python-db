import sqlite3
import warnings
from typing import Iterable, Union

from privex.db.query.asyncx.sqlite import _zip_cols
from privex.db.query.base import BaseQueryBuilder, QueryMode


class SqliteQueryBuilder(BaseQueryBuilder):
    def fetch_next(self, query_mode=QueryMode.ROW_DICT) -> Union[dict, tuple, None]:
        if not self._is_executed:
            self.execute()
        res = self.cursor.fetchone()
        if len(res) > 0 and query_mode == QueryMode.ROW_DICT:
            res = _zip_cols(self.cursor, tuple(res))
        return res
    
    def fetch(self, query_mode=QueryMode.ROW_DICT) -> Union[dict, tuple, None]:
        if self.conn is None:
            raise Exception('Please set SqliteQueryBuilder.connection to an sqlite3 connection')
        with self.cursor as cur:
            self.execute()
            res = cur.fetchone()
            if len(res) > 0 and query_mode == QueryMode.ROW_DICT:
                res = _zip_cols(cur, tuple(res))
            # cur.close()
        return res

    Q_DEFAULT_PLACEHOLDER = '?'
    Q_PRE_QUERY = ''
    connection: sqlite3.Connection = None
    
    @property
    def conn(self) -> sqlite3.Connection:
        return self.connection
    
    def build_query(self) -> str:
        return self._build_query()

    def all(self, query_mode=QueryMode.ROW_DICT) -> Union[Iterable[dict], Iterable[tuple]]:
        if self.conn is None:
            raise Exception('Please set SqliteQueryBuilder.connection to an sqlite3 connection')
        # cur = self.conn.cursor()
        # for res in cur.execute(self.build_query(), self.where_clauses_values):
        with self.cursor as cur:
            for res in self.execute():
                if query_mode == QueryMode.ROW_DICT:
                    yield _zip_cols(cur, res)
                else:
                    yield res
            # res = cur.fetchall()
            # orig_res = list(res)
            # if len(res) > 0:
            #     res = [self._zip_cols(cur, r) for r in orig_res]
        # cur.close()
        # return res

