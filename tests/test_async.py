import pytest
import sqlite3
from privex.db import QueryMode
from tests.base import ExampleAsyncWrapper


wrp: ExampleAsyncWrapper = ExampleAsyncWrapper()

# To keep the shared memory persistent database alive, we open an sqlite3 connection which isn't actually used.
_conn = sqlite3.connect(ExampleAsyncWrapper.DEFAULT_DB, uri=True)


@pytest.fixture()
async def setup_teardown() -> None:
    global wrp
    await wrp.close_cursor()
    wrp = ExampleAsyncWrapper()
    # wrp = ExampleAsyncWrapper()
    await wrp.recreate_schemas()
    yield ""
    await wrp.drop_schemas()


# @pytest.fixture()
# def tearDown() -> None:
#     wrp.drop_schemas()


def assertEqual(param, param1):
    assert param == param1


def assertIn(param, container):
    assert param in container


def assertNotIn(param, container):
    assert param not in container


@pytest.mark.asyncio
async def test_all_call(setup_teardown):
    b = wrp.builder('users')
    await wrp.insert_user('John', 'Doe')
    await wrp.insert_user('Dave', 'Johnson')
    
    # res = list(await b.all())
    res = [row async for row in b.all()]
    assertEqual(res[0]['first_name'], 'John')
    assertEqual(res[0]['last_name'], 'Doe')
    assertEqual(res[1]['first_name'], 'Dave')
    assertEqual(res[1]['last_name'], 'Johnson')


@pytest.mark.asyncio
async def test_where_call(setup_teardown):
    b = wrp.builder('users')
    await wrp.insert_user('John', 'Doe')
    await wrp.insert_user('Dave', 'Johnson')
    await wrp.insert_user('Jane', 'Smith')
    
    res = await b.where('first_name', 'Dave').fetch()
    assertEqual(res['first_name'], 'Dave')
    assertEqual(res['last_name'], 'Johnson')


@pytest.mark.asyncio
async def test_group_call(setup_teardown):
    b = wrp.builder('users')
    await wrp.insert_user('John', 'Doe')
    await wrp.insert_user('John', 'Johnson')
    await wrp.insert_user('John', 'Smith')
    await wrp.insert_user('Dave', 'Johnson')
    await wrp.insert_user('Jane', 'Smith')
    
    b.select('first_name', 'COUNT(first_name)').where('first_name', 'John').group_by('first_name')
    
    res = await b.fetch(query_mode=QueryMode.ROW_TUPLE)
    assertEqual(res[0], 'John')
    assertEqual(res[1], 3)


@pytest.mark.asyncio
async def test_iterate_builder(setup_teardown):
    """
    Test obtaining SqliteQueryBuilder results by iterating over the builder object itself with a for loop
    """
    b = wrp.builder('users')
    ex_users = wrp.example_users
    for u in ex_users:
        await wrp.insert_user(u.first_name, u.last_name)
    
    for i, row in enumerate(b):
        assertEqual(row['first_name'], ex_users[i].first_name)
        assertEqual(row['last_name'], ex_users[i].last_name)


@pytest.mark.asyncio
async def test_index_builder(setup_teardown):
    """
    Test obtaining SqliteQueryBuilder results by accessing an index of the builder object
    """
    b = wrp.builder('users')
    ex_users = wrp.example_users
    for u in ex_users:
        await wrp.insert_user(u.first_name, u.last_name)
    
    for i in range(0, 3):
        assertEqual(b[i]['first_name'], ex_users[i].first_name)
        assertEqual(b[i]['last_name'], ex_users[i].last_name)


@pytest.mark.asyncio
async def test_generator_builder(setup_teardown):
    """
    Test obtaining SqliteQueryBuilder results by calling :func:`next` on the builder object (like a generator)
    """
    
    ex_users = wrp.example_users
    for u in ex_users:
        await wrp.insert_user(u.first_name, u.last_name)
    
    async with wrp.builder('users') as b:
        for i in range(0, len(ex_users)):
            user = await b.__anext__()
            assertEqual(user['first_name'], ex_users[i].first_name)
            assertEqual(user['last_name'], ex_users[i].last_name)


@pytest.mark.asyncio
async def test_tables_created(setup_teardown):
    w = wrp
    assertEqual(w.db, ExampleAsyncWrapper.DEFAULT_DB)
    tables = await w.list_tables()
    assertIn('users', tables)
    assertIn('items', tables)


@pytest.mark.asyncio
async def test_tables_drop(setup_teardown):
    w = wrp
    tables = await w.list_tables()
    assertIn('users', tables)
    assertIn('items', tables)

    await w.drop_schemas()
    tables = await w.list_tables()
    assertNotIn('users', tables)
    assertNotIn('items', tables)


@pytest.mark.asyncio
async def test_insert_find_user(setup_teardown):
    w = wrp
    w.query_mode = 'flat'
    res = await w.insert_user('John', 'Doe')
    assertEqual(res.rowcount, 1)
    user = await w.find_user(res.lastrowid)
    print('User is:', user)
    assertEqual(user[1], 'John')
    assertEqual(user[2], 'Doe')


@pytest.mark.asyncio
async def test_action_update(setup_teardown):
    w = wrp
    w.query_mode = 'dict'
    res = await w.insert_user('John', 'Doe')
    last_id = res.lastrowid
    rows = await w.action("UPDATE users SET last_name = ? WHERE first_name = ?", ['Smith', 'John'])
    assertEqual(rows, 1)
    john = await w.find_user(last_id)
    assertEqual(john['last_name'], 'Smith')


@pytest.mark.asyncio
async def test_find_user_dict_mode(setup_teardown):
    w = wrp
    w.query_mode = 'dict'
    res = await w.insert_user('John', 'Doe')
    assertEqual(res.rowcount, 1)
    user = await w.find_user(res.lastrowid)
    assertEqual(user['first_name'], 'John')
    assertEqual(user['last_name'], 'Doe')


def assertIsNone(param):
    assert param is None


@pytest.mark.asyncio
async def test_find_user_nonexistent(setup_teardown):
    w = wrp
    user = await w.find_user(99)
    assertIsNone(user)


@pytest.mark.asyncio
async def test_get_users_tuple(setup_teardown):
    w = wrp
    w.query_mode = 'flat'
    await w.insert_user('John', 'Doe')
    await w.insert_user('Jane', 'Doe')
    await w.insert_user('Dave', 'Johnson')

    users = list(await w.get_users())
    assertEqual(len(users), 3)
    assertEqual(users[0][1], 'John')

    assertEqual(users[1][1], 'Jane')
    assertEqual(users[1][2], 'Doe')

    assertEqual(users[2][2], 'Johnson')


@pytest.mark.asyncio
async def test_get_users_dict(setup_teardown):
    w = wrp
    w.query_mode = 'dict'

    await w.insert_user('John', 'Doe')
    await w.insert_user('Jane', 'Doe')
    await w.insert_user('Dave', 'Johnson')

    users = list(await w.get_users())
    assertEqual(len(users), 3)
    assertEqual(users[0]['first_name'], 'John')

    assertEqual(users[1]['first_name'], 'Jane')
    assertEqual(users[1]['last_name'], 'Doe')

    assertEqual(users[2]['last_name'], 'Johnson')


@pytest.mark.asyncio
async def test_insert_helper(setup_teardown):
    w = wrp
    w.query_mode = 'dict'
    res = await w.insert('users', first_name='Dave', last_name='Johnson')
    assertEqual(res.lastrowid, 1)

    user = await w.find_user(res.lastrowid)
    assertEqual(user['first_name'], 'Dave')
    assertEqual(user['last_name'], 'Johnson')
