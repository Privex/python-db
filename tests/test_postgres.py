"""
Tests related to :class:`.PostgresQueryBuilder`, :class:`.ExamplePostgresWrapper` and :class:`.PostgresWrapper`
"""
import warnings
import pytest
import logging
from typing import List, Tuple
from privex.helpers import Mocker
from tests.base import *
from os import getenv as env
from tests.base import _TestWrapperMixin

log = logging.getLogger(__name__)


try:
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from privex.db.postgres import PostgresWrapper
    from privex.db import PostgresQueryBuilder

    HAS_PSYCOPG = True
except (ImportError, AttributeError, NameError):
    PostgresWrapper, PostgresQueryBuilder = object, object
    psycopg2 = Mocker()
    psycopg2.add_mock_module('extensions')
    HAS_PSYCOPG = False

DB_USER = env('DB_USER', 'root')
DB_NAME = env('DB_NAME', 'privex_py_db')
DB_PASS = env('DB_PASS')
DB_HOST = env('DB_HOST')

pg_conn = None

if HAS_PSYCOPG:
    try:
        pg_conn: psycopg2.extensions.connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        pg_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    except (psycopg2.Error, psycopg2.DatabaseError) as e:
        pg_conn = None
        warnings.warn(f"Error while connecting to PostgreSQL: {type(e)} - {str(e)}")

if HAS_PSYCOPG:
    class ExamplePostgresWrapper(PostgresWrapper, _TestWrapperMixin):
        """
        A wrapper around :class:`.PostgresWrapper` and :class:`.TestWrapperMixin` for use in Postgres tests.

        * Sets the default database to ``privex_py_db``
        * Creates the table ``users``
        * Includes two helper query methods :py:meth:`.insert_user` and :py:meth:`.find_user`

        """
        DEFAULT_DB: str = 'privex_py_db'
        SCHEMAS: List[Tuple[str, str]] = [
            (
                'users',
                "CREATE TABLE users (id SERIAL PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255));"
            ),
        ]
        
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # self.cursor_cls = super().cursor_cls
        
        def insert_user(self, first_name, last_name):
            c = self.get_cursor()
            c.execute(
                "INSERT INTO users (first_name, last_name) VALUES (%s, %s);",
                [first_name, last_name]
            )
            return c
        
        def find_user(self, id: int):
            return self.fetchone("SELECT * FROM users WHERE id = %s;", [id])
else:
    ExamplePostgresWrapper = Mocker()


class BasePostgresTest(PrivexDBTestBase):
    """
    Shared base class for Postgres tests.
    
    * Sets up :class:`.ExamplePostgresWrapper` into the attribute :attr:`.wrp`
    * Before each test (:meth:`.setUp`) it deletes and recreates the tables to avoid leftover tables/rows
    * After each test (:meth:`.tearDown`) it drops all tables to ensure no leftover tables once the tests are done.
    
    """
    conn = pg_conn
    """Holds the module's Postgres connection"""
    wrp: ExamplePostgresWrapper
    """Holds a :class:`.ExamplePostgresWrapper` instance for use by test cases"""
    
    @classmethod
    def setUpClass(cls):
        """Sets up a :class:`.ExamplePostgresWrapper` instance under :attr:`.wrp` for use by tests"""
        cls.wrp = ExamplePostgresWrapper(db=DB_NAME, db_user=DB_USER, db_host=DB_HOST, db_pass=DB_PASS)
    
    def setUp(self) -> None:
        """Deletes and recreates all tables to avoid leftover tables/rows"""
        self.wrp.recreate_schemas()
    
    def tearDown(self) -> None:
        """Deletes all tables after each test finises to avoid leftover tables/rows"""
        self.wrp.drop_schemas()

    @property
    def _builder(self) -> PostgresQueryBuilder:
        return self.wrp.builder('users')

    def _insert_user(self, first_name, last_name) -> psycopg2.extensions.cursor:
        """Helper function to insert a user into the database"""
        log.debug("Inserting user. First: %s    Last: %s", first_name, last_name)
        return self.wrp.insert_user(first_name, last_name)


@pytest.mark.skipif(pg_conn is None, reason="Failed to connect to PostgreSQL")
@pytest.mark.skipif(HAS_PSYCOPG is False, reason="Library 'psycopg2' is not installed...")
class TestPostgresWrapper(BasePostgresTest):
    def test_tables_created(self):
        w = self.wrp
        self.assertEqual(w.db, DB_NAME)
        tables = w.list_tables()
        self.assertIn('users', tables)
    
    def test_tables_drop(self):
        w = self.wrp
        tables = w.list_tables()
        self.assertIn('users', tables)
        
        w.drop_schemas()
        tables = w.list_tables()
        self.assertNotIn('users', tables)

    def test_insert_find_user(self):
        w = self.wrp
        w.query_mode = 'flat'
        res = w.insert_user('John', 'Doe')
        self.assertEqual(res.rowcount, 1)
        last_id = w.last_insert_id('users')
        user = w.find_user(last_id)
        self.assertEqual(user[1], 'John')
        self.assertEqual(user[2], 'Doe')

    def test_action_update(self):
        w = self.wrp
        w.query_mode = 'dict'
        w.insert_user('John', 'Doe')
        last_id = w.last_insert_id('users')
        rows = w.action("UPDATE users SET last_name = %s WHERE first_name = %s", ['Smith', 'John'])
        self.assertEqual(rows, 1)
        john = w.find_user(last_id)
        self.assertEqual(john['last_name'], 'Smith')

    def test_find_user_dict_mode(self):
        w = self.wrp
        w.query_mode = 'dict'
        res = w.insert_user('John', 'Doe')
        self.assertEqual(res.rowcount, 1)
        user = w.find_user(w.last_insert_id('users'))
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
        self.assertTrue(hasattr(res, 'lastrowid'))
        self.assertTrue(hasattr(res, 'rowcount'))
    
        user = w.find_user(w.last_insert_id('users'))
        self.assertEqual(user['first_name'], 'Dave')
        self.assertEqual(user['last_name'], 'Johnson')


@pytest.mark.skipif(pg_conn is None, reason="Failed to connect to PostgreSQL")
@pytest.mark.skipif(HAS_PSYCOPG is False, reason="Library 'psycopg2' is not installed...")
class TestPostgresBuilder(BasePostgresTest):
    def test_query_all(self):
        """Build a select all query using :class:`.PostgresQueryBuilder` and confirm the built query looks correct"""
        b = self._builder
        q = b.build_query().strip()
        self.assertEqual(f'{b.Q_PRE_QUERY} SELECT * FROM users;', q)
    
    def test_query_where_first_name_last_name(self):
        """Build a 'where AND' query using :class:`.PostgresQueryBuilder` and confirm the built query looks correct"""
        b = self._builder
        b.where('first_name', 'John').where('last_name', 'Doe')
        q = b.build_query().strip()
        self.assertEqual(f'{b.Q_PRE_QUERY} SELECT * FROM users WHERE first_name = %s AND last_name = %s;', q)
    
    def test_query_select_col_where(self):
        """
        Build a query selecting specific columns plus a 'where AND' clause using :class:`.PostgresQueryBuilder`
        and confirm the built query looks correct
        """
        b = self._builder
        b.select('first_name')
        b.where('first_name', 'John').where('last_name', 'Doe')
        q = b.build_query().strip()
        self.assertEqual(f'{b.Q_PRE_QUERY} SELECT first_name FROM users WHERE first_name = %s AND last_name = %s;', q)
    
    def test_query_select_col_where_order(self):
        """
        Build a query selecting specific columns, a 'where AND' clause, and an 'ORDER BY' clause
        using :class:`.PostgresQueryBuilder` and confirm the built query looks correct
        """
        b = self._builder
        b.select('first_name').where('first_name', 'John').where('last_name', 'Doe').order('first_name')
        q = b.build_query().strip()
        self.assertEqual(
            f'{b.Q_PRE_QUERY} SELECT first_name FROM users WHERE first_name = %s AND '
            f'last_name = %s ORDER BY first_name DESC;',
            q
        )
    
    def test_query_select_col_where_group(self):
        """
        Build a complex select+where+group_by query using :class:`.PostgresQueryBuilder` and confirm the
        built query looks correct
        """
        b = self._builder
        b.select('first_name', 'COUNT(first_name)').where('first_name', 'John') \
            .where('last_name', 'Doe').group_by('first_name')
        
        q = b.build_query().strip()
        self.assertEqual(
            f'{b.Q_PRE_QUERY} SELECT first_name, COUNT(first_name) FROM users WHERE first_name = %s AND ' +
            'last_name = %s GROUP BY first_name;',
            q
        )
    
    def test_all_call(self):
        """
        Insert two users, then verify they're returned from an ``.all()`` call using :class:`.PostgresQueryBuilder`
        """
        b = self._builder
        self._insert_user('John', 'Doe')
        self._insert_user('Dave', 'Johnson')
        
        res = list(b.all())
        self.assertEqual(res[0]['first_name'], 'John')
        self.assertEqual(res[0]['last_name'], 'Doe')
        self.assertEqual(res[1]['first_name'], 'Dave')
        self.assertEqual(res[1]['last_name'], 'Johnson')
    
    def test_where_call(self):
        """
        Insert three users, then retrieve Dave using ``.where()`` + ``.fetch()`` call on :class:`.PostgresQueryBuilder`
        """
        b = self._builder
        self._insert_user('John', 'Doe')
        self._insert_user('Dave', 'Johnson')
        self._insert_user('Jane', 'Smith')
        
        res = b.where('first_name', 'Dave').fetch()
        self.assertEqual(res['first_name'], 'Dave')
        self.assertEqual(res['last_name'], 'Johnson')
    
    def test_group_call(self):
        """
        Insert 5 users with 3 "John"s, then run a select+where+group_by query and confirm COUNT returns 3 johns
        """
        b = self._builder
        
        self._insert_user('John', 'Doe')
        self._insert_user('John', 'Johnson')
        self._insert_user('John', 'Smith')
        self._insert_user('Dave', 'Johnson')
        self._insert_user('Jane', 'Smith')
        
        b.select('first_name', 'COUNT(first_name)').where('first_name', 'John').group_by('first_name')
        log.debug("Built query: %s", b.build_query())
        log.debug("Running fetch")
        res = b.fetch(query_mode=QueryMode.ROW_TUPLE)
        log.debug("Comparing results")
        self.assertEqual(res[0], 'John')
        self.assertEqual(res[1], 3)

    def test_iterate_builder(self):
        """
        Test obtaining PostgresBuilder results by iterating over the builder object itself with a for loop
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
        Test obtaining PostgresBuilder results by accessing an index of the builder object
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
        Test obtaining PostgresBuilder results by calling :func:`next` on the builder object (like a generator)
        """
        b = self.wrp.builder('users')
    
        ex_users = self.wrp.example_users
        for u in ex_users:
            self.wrp.insert_user(u.first_name, u.last_name)
    
        for i in range(0, len(ex_users)):
            user = next(b)
            self.assertEqual(user['first_name'], ex_users[i].first_name)
            self.assertEqual(user['last_name'], ex_users[i].last_name)

