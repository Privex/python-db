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
import dotenv
from os import getenv as env
from typing import List, Tuple
from unittest import TestCase
from privex.loghelper import LogHelper
from privex.helpers import dictable_namedtuple
from privex.db import SqliteWrapper, BaseQueryBuilder, SqliteQueryBuilder, QueryMode
from privex.db import _setup_logging

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


class PrivexDBTestBase(TestCase):
    """
    Base class for all privex-db test classes. Includes :meth:`.tearDown` to reset database after each test.
    """

    def setUp(self) -> None:
        self.wrp = ExampleWrapper()

    def tearDown(self) -> None:
        self.wrp.drop_schemas()


__all__ = [
    'PrivexDBTestBase', 'SqliteWrapper', 'BaseQueryBuilder', 'SqliteQueryBuilder', 'QueryMode',
    'ExampleWrapper', 'LOG_LEVEL', 'LOG_FORMATTER'
]
"""
We manually specify __all__ so that we can safely use ``from tests.base import *`` within each test file.
"""

User = dictable_namedtuple('User', 'first_name last_name')


class _TestWrapperMixin:
    example_users = [
        User('John', 'Doe'),
        User('Jane', 'Smith'),
        User('John', 'Johnson'),
        User('Dave', 'Johnson'),
        User('John', 'Smith'),
    ]
    
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


