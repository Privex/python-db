"""
Tests related to :class:`.SqliteWrapper` / :class:`.ExampleWrapper`
"""
# from unittest import TestCase
from tests.base import *


class TestSQLiteWrapper(PrivexDBTestBase):
    
    def test_tables_created(self):
        w = self.wrp
        self.assertEqual(w.db, ':memory:')
        tables = w.list_tables()
        self.assertIn('users', tables)
        self.assertIn('items', tables)

    def test_tables_drop(self):
        w = self.wrp
        tables = w.list_tables()
        self.assertIn('users', tables)
        self.assertIn('items', tables)
        
        w.drop_schemas()
        tables = w.list_tables()
        self.assertNotIn('users', tables)
        self.assertNotIn('items', tables)
    
    def test_insert_find_user(self):
        w = self.wrp
        w.query_mode = 'flat'
        res = w.insert_user('John', 'Doe')
        self.assertEqual(res.rowcount, 1)
        user = w.find_user(res.lastrowid)
        self.assertEqual(user[1], 'John')
        self.assertEqual(user[2], 'Doe')

    def test_action_update(self):
        w = self.wrp
        w.query_mode = 'dict'
        res = w.insert_user('John', 'Doe')
        last_id = res.lastrowid
        rows = w.action("UPDATE users SET last_name = ? WHERE first_name = ?", ['Smith', 'John'])
        self.assertEqual(rows, 1)
        john = w.find_user(last_id)
        self.assertEqual(john['last_name'], 'Smith')

    def test_find_user_dict_mode(self):
        w = self.wrp
        w.query_mode = 'dict'
        res = w.insert_user('John', 'Doe')
        self.assertEqual(res.rowcount, 1)
        user = w.find_user(res.lastrowid)
        self.assertEqual(user['first_name'], 'John')
        self.assertEqual(user['last_name'], 'Doe')

    def test_find_user_nonexistent(self):
        w = self.wrp
        user = w.find_user(99)
        self.assertIsNone(user)

    def test_get_users_tuple(self):
        w = self.wrp
        w.query_mode = 'flat'
        w.insert_user('John', 'Doe')
        w.insert_user('Jane', 'Doe')
        w.insert_user('Dave', 'Johnson')
        
        users = list(w.get_users())
        self.assertEqual(len(users), 3)
        self.assertEqual(users[0][1], 'John')
        
        self.assertEqual(users[1][1], 'Jane')
        self.assertEqual(users[1][2], 'Doe')
        
        self.assertEqual(users[2][2], 'Johnson')

    def test_get_users_dict(self):
        w = self.wrp
        w.query_mode = 'dict'
        
        w.insert_user('John', 'Doe')
        w.insert_user('Jane', 'Doe')
        w.insert_user('Dave', 'Johnson')
    
        users = list(w.get_users())
        self.assertEqual(len(users), 3)
        self.assertEqual(users[0]['first_name'], 'John')
    
        self.assertEqual(users[1]['first_name'], 'Jane')
        self.assertEqual(users[1]['last_name'], 'Doe')
    
        self.assertEqual(users[2]['last_name'], 'Johnson')

    def test_insert_helper(self):
        w = self.wrp
        w.query_mode = 'dict'
        res = w.insert('users', first_name='Dave', last_name='Johnson')
        self.assertEqual(res.lastrowid, 1)

        user = w.find_user(res.lastrowid)
        self.assertEqual(user['first_name'], 'Dave')
        self.assertEqual(user['last_name'], 'Johnson')
