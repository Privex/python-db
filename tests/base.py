"""
This file contains code shared between tests, such as :class:`.PrivexDBTestBase` which is the base class shared
by all unit tests.

**Copyright**::

    +===================================================+
    |                 © 2019 Privex Inc.                |
    |               https://www.privex.io               |
    +===================================================+
    |                                                   |
    |        Django Database Lock Manager               |
    |        License: X11/MIT                           |
    |                                                   |
    |        Core Developer(s):                         |
    |                                                   |
    |          (+)  Chris (@someguy123) [Privex]        |
    |                                                   |
    +===================================================+

"""
import logging
import sqlite3
import warnings

import dotenv
from os import getenv as env
from typing import List, Tuple
from unittest import TestCase
from privex.loghelper import LogHelper
from privex.helpers import dictable_namedtuple, Mocker
from privex.db import SqliteWrapper, BaseQueryBuilder, SqliteQueryBuilder, QueryMode
from privex.db import _setup_logging
from privex.db.sqlite import SqliteAsyncWrapper

try:
    dotenv.read_dotenv()
except AttributeError:
    dotenv.load_dotenv()


LOG_LEVEL = env('LOG_LEVEL')
LOG_LEVEL = logging.getLevelName(str(LOG_LEVEL).upper()) if LOG_LEVEL is not None else logging.WARNING
_setup_logging(LOG_LEVEL)
LOG_FORMATTER = logging.Formatter('[%(asctime)s]: %(name)-55s -> %(funcName)-20s : %(levelname)-8s:: %(message)s')
_lh = LogHelper('privex.db.tests', handler_level=LOG_LEVEL, formatter=LOG_FORMATTER)
_lh.copy_logger('privex.db')
_lh.add_console_handler()
log = _lh.get_logger()


class PrivexTestBase(TestCase):
    pass


class PrivexDBTestBase(PrivexTestBase):
    """
    Base class for all privex-db test classes. Includes :meth:`.tearDown` to reset database after each test.
    """

    def setUp(self) -> None:
        self.wrp = ExampleWrapper()

    def tearDown(self) -> None:
        self.wrp.drop_schemas()


__all__ = [
    'PrivexDBTestBase', 'SqliteWrapper', 'BaseQueryBuilder', 'SqliteQueryBuilder', 'QueryMode',
    'ExampleWrapper', 'LOG_LEVEL', 'LOG_FORMATTER', 'User', 'example_users'
]
"""
We manually specify __all__ so that we can safely use ``from tests.base import *`` within each test file.
"""

User = dictable_namedtuple('User', 'first_name last_name')

example_users = [
    User('John', 'Doe'),
    User('Jane', 'Smith'),
    User('John', 'Johnson'),
    User('Dave', 'Johnson'),
    User('John', 'Smith'),
]


class _TestWrapperMixin:
    example_users = example_users
    
    def __init__(self, *args, **kwargs):
        super(_TestWrapperMixin, self).__init__(*args, **kwargs)
    
    def get_items(self):
        return self.fetchall("SELECT * FROM items;")
    
    def find_item(self, id: int):
        return self.fetchone("SELECT * FROM items WHERE id = ?;", [id])
    
    def get_users(self):
        return self.fetchall("SELECT * FROM users;")
    
    def insert_user(self, first_name, last_name) -> sqlite3.Cursor:
        c = self.conn.cursor()
        return c.execute(
            "INSERT INTO users (first_name, last_name) "
            "VALUES (?, ?);",
            [first_name, last_name]
        )
    
    def insert_item(self, name) -> sqlite3.Cursor:
        c = self.conn.cursor()
        return c.execute(
            "INSERT INTO items (name) VALUES (?);",
            [name]
        )
    
    def find_user(self, id: int):
        return self.fetchone("SELECT * FROM users WHERE id = ?;", [id])


class ExampleWrapper(SqliteWrapper, _TestWrapperMixin):
    DEFAULT_DB: str = ':memory:'
    SCHEMAS: List[Tuple[str, str]] = [
        (
            'users',
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, first_name TEXT, last_name TEXT);"
        ),
        (
            'items', "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);"
        ),
    ]

    def __init__(self, *args, **kwargs):
        super(ExampleWrapper, self).__init__(*args, **kwargs)

try:
    import aiosqlite
    HAS_ASYNC = True
    
    class _TestAsyncWrapperMixin:
        def __init__(self, *args, **kwargs):
            super(_TestWrapperMixin, self).__init__(*args, **kwargs)
        
        async def get_items(self):
            return await self.fetchall("SELECT * FROM items;")
        
        async def find_item(self, id: int):
            return await self.fetchone("SELECT * FROM items WHERE id = ?;", [id])
        
        async def get_users(self):
            return await self.fetchall("SELECT * FROM users;")
        
        async def insert_user(self, first_name, last_name) -> aiosqlite.Cursor:
            # c = await self.conn.cursor()
            res = await self.execute(
                "INSERT INTO users (first_name, last_name) "
                "VALUES (?, ?);",
                [first_name, last_name], fetch='no'
            )
            return res[1]
        
        async def insert_item(self, name) -> sqlite3.Cursor:
            # c = await self.conn.cursor()
            res = await self.execute(
                "INSERT INTO items (name) VALUES (?);",
                [name]
            )
            return res[1]
        
        async def find_user(self, id: int):
            return await self.fetchone("SELECT * FROM users WHERE id = ?;", [id])
    
    
    class ExampleAsyncWrapper(SqliteAsyncWrapper, _TestAsyncWrapperMixin):
        example_users = example_users
    
        DEFAULT_DB: str = 'file::memory:?cache=privexdbtests'
        SCHEMAS: List[Tuple[str, str]] = [
            (
                'users',
                "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, first_name TEXT, last_name TEXT);"
            ),
            (
                'items', "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);"
            ),
        ]
    
        def __init__(self, *args, **kwargs):
            super(ExampleAsyncWrapper, self).__init__(*args, **kwargs)


    # class PrivexAsyncTestBase(PrivexTestBase):
    #     def setUp(self) -> None:
    #         self.wrp = ExampleAsyncWrapper()
    #
    #     def tearDown(self) -> None:
    #         self.wrp.drop_schemas()

    # __all__ += ['PrivexAsyncTestBase', 'ExampleAsyncWrapper']
    __all__ += ['ExampleAsyncWrapper']

except ImportError:
    HAS_ASYNC = False
    # PrivexAsyncTestBase = Mocker()
    ExampleAsyncWrapper = Mocker()
    warnings.warn("Could not import 'aiosqlite'. ExampleAsyncWrapper will not be available.")
