"""
Tests related to :class:`.SqliteQueryBuilder` and :class:`.ExampleWrapper`
"""
from unittest import TestCase

import pytest

from privex.db.sqlite import SqliteAsyncWrapper
from tests.base import *
import logging
import nest_asyncio
nest_asyncio.apply()

log = logging.getLogger(__name__)


class TestSQLiteBuilder(PrivexDBTestBase):
    def test_query_all(self):
        b = self.wrp.builder('users')
        q = b.build_query().strip()
        self.assertEqual('SELECT * FROM users;', q)

    def test_query_where_first_name_last_name(self):
        b = self.wrp.builder('users')
        b.where('first_name', 'John').where('last_name', 'Doe')
        q = b.build_query().strip()
        self.assertEqual('SELECT * FROM users WHERE first_name = ? AND last_name = ?;', q)

    def test_query_select_col_where(self):
        b = self.wrp.builder('users')
        b.select('first_name')
        b.where('first_name', 'John').where('last_name', 'Doe')
        q = b.build_query().strip()
        self.assertEqual('SELECT first_name FROM users WHERE first_name = ? AND last_name = ?;', q)

    def test_query_select_col_where_order(self):
        b = self.wrp.builder('users')
        b.select('first_name').where('first_name', 'John').where('last_name', 'Doe').order('first_name')
        q = b.build_query().strip()
        self.assertEqual(
            'SELECT first_name FROM users WHERE first_name = ? AND last_name = ? ORDER BY first_name DESC;',
            q
        )

    def test_query_select_col_where_group(self):
        b = self.wrp.builder('users')
        b.select('first_name', 'COUNT(first_name)').where('first_name', 'John')\
            .where('last_name', 'Doe').group_by('first_name')
        
        q = b.build_query().strip()
        self.assertEqual(
            'SELECT first_name, COUNT(first_name) FROM users WHERE first_name = ? AND '
            'last_name = ? GROUP BY first_name;',
            q
        )
    
    def test_all_call(self):
        b = self.wrp.builder('users')
        self.wrp.insert_user('John', 'Doe')
        self.wrp.insert_user('Dave', 'Johnson')
        
        res = list(b.all())
        self.assertEqual(res[0]['first_name'], 'John')
        self.assertEqual(res[0]['last_name'], 'Doe')
        self.assertEqual(res[1]['first_name'], 'Dave')
        self.assertEqual(res[1]['last_name'], 'Johnson')

    def test_where_call(self):
        b = self.wrp.builder('users')
        self.wrp.insert_user('John', 'Doe')
        self.wrp.insert_user('Dave', 'Johnson')
        self.wrp.insert_user('Jane', 'Smith')

        res = b.where('first_name', 'Dave').fetch()
        self.assertEqual(res['first_name'], 'Dave')
        self.assertEqual(res['last_name'], 'Johnson')

    def test_group_call(self):
        b = self.wrp.builder('users')
        self.wrp.insert_user('John', 'Doe')
        self.wrp.insert_user('John', 'Johnson')
        self.wrp.insert_user('John', 'Smith')
        self.wrp.insert_user('Dave', 'Johnson')
        self.wrp.insert_user('Jane', 'Smith')

        b.select('first_name', 'COUNT(first_name)').where('first_name', 'John').group_by('first_name')
        
        res = b.fetch(query_mode=QueryMode.ROW_TUPLE)
        self.assertEqual(res[0], 'John')
        self.assertEqual(res[1], 3)

    def test_iterate_builder(self):
        """
        Test obtaining SqliteQueryBuilder results by iterating over the builder object itself with a for loop
        """
        b = self.wrp.builder('users')
        ex_users = self.wrp.example_users
        for u in ex_users:
            self.wrp.insert_user(u.first_name, u.last_name)
        
        for i, row in enumerate(b):
            self.assertEqual(row['first_name'], ex_users[i].first_name)
            self.assertEqual(row['last_name'], ex_users[i].last_name)

    def test_index_builder(self):
        """
        Test obtaining SqliteQueryBuilder results by accessing an index of the builder object
        """
        b = self.wrp.builder('users')
        ex_users = self.wrp.example_users
        for u in ex_users:
            self.wrp.insert_user(u.first_name, u.last_name)
        
        for i in range(0, 3):
            self.assertEqual(b[i]['first_name'], ex_users[i].first_name)
            self.assertEqual(b[i]['last_name'], ex_users[i].last_name)

    def test_generator_builder(self):
        """
        Test obtaining SqliteQueryBuilder results by calling :func:`next` on the builder object (like a generator)
        """
        b = self.wrp.builder('users')
        
        ex_users = self.wrp.example_users
        for u in ex_users:
            self.wrp.insert_user(u.first_name, u.last_name)

        for i in range(0, len(ex_users)):
            user = next(b)
            self.assertEqual(user['first_name'], ex_users[i].first_name)
            self.assertEqual(user['last_name'], ex_users[i].last_name)

#
# class TestAsyncSQLiteBuilder(TestCase):
#     wrp: ExampleAsyncWrapper
#
#     def setUp(self) -> None:
#         self.wrp = ExampleAsyncWrapper()
#
#     def tearDown(self) -> None:
#         self.wrp.drop_schemas()
#
#     @pytest.mark.asyncio
#     async def test_all_call(self):
#         b = self.wrp.builder('users')
#         await self.wrp.insert_user('John', 'Doe')
#         await self.wrp.insert_user('Dave', 'Johnson')
#
#         res = list(await b.all())
#         self.assertEqual(res[0]['first_name'], 'John')
#         self.assertEqual(res[0]['last_name'], 'Doe')
#         self.assertEqual(res[1]['first_name'], 'Dave')
#         self.assertEqual(res[1]['last_name'], 'Johnson')
#
#     @pytest.mark.asyncio
#     async def test_where_call(self):
#         b = self.wrp.builder('users')
#         await self.wrp.insert_user('John', 'Doe')
#         await self.wrp.insert_user('Dave', 'Johnson')
#         await self.wrp.insert_user('Jane', 'Smith')
#
#         res = await b.where('first_name', 'Dave').fetch()
#         self.assertEqual(res['first_name'], 'Dave')
#         self.assertEqual(res['last_name'], 'Johnson')
#
#     @pytest.mark.asyncio
#     async def test_group_call(self):
#         b = self.wrp.builder('users')
#         await self.wrp.insert_user('John', 'Doe')
#         await self.wrp.insert_user('John', 'Johnson')
#         await self.wrp.insert_user('John', 'Smith')
#         await self.wrp.insert_user('Dave', 'Johnson')
#         await self.wrp.insert_user('Jane', 'Smith')
#
#         b.select('first_name', 'COUNT(first_name)').where('first_name', 'John').group_by('first_name')
#
#         res = await b.fetch(query_mode=QueryMode.ROW_TUPLE)
#         self.assertEqual(res[0], 'John')
#         self.assertEqual(res[1], 3)
#
#     @pytest.mark.asyncio
#     async def test_iterate_builder(self):
#         """
#         Test obtaining SqliteQueryBuilder results by iterating over the builder object itself with a for loop
#         """
#         b = self.wrp.builder('users')
#         ex_users = self.wrp.example_users
#         for u in ex_users:
#             await self.wrp.insert_user(u.first_name, u.last_name)
#
#         for i, row in enumerate(b):
#             self.assertEqual(row['first_name'], ex_users[i].first_name)
#             self.assertEqual(row['last_name'], ex_users[i].last_name)
#
#     @pytest.mark.asyncio
#     async def test_index_builder(self):
#         """
#         Test obtaining SqliteQueryBuilder results by accessing an index of the builder object
#         """
#         b = self.wrp.builder('users')
#         ex_users = self.wrp.example_users
#         for u in ex_users:
#             await self.wrp.insert_user(u.first_name, u.last_name)
#
#         for i in range(0, 3):
#             self.assertEqual(b[i]['first_name'], ex_users[i].first_name)
#             self.assertEqual(b[i]['last_name'], ex_users[i].last_name)
#
#     @pytest.mark.asyncio
#     async def test_generator_builder(self):
#         """
#         Test obtaining SqliteQueryBuilder results by calling :func:`next` on the builder object (like a generator)
#         """
#         b = self.wrp.builder('users')
#
#         ex_users = self.wrp.example_users
#         for u in ex_users:
#             await self.wrp.insert_user(u.first_name, u.last_name)
#
#         for i in range(0, len(ex_users)):
#             user = next(b)
#             self.assertEqual(user['first_name'], ex_users[i].first_name)
#             self.assertEqual(user['last_name'], ex_users[i].last_name)
