"""
This module contains test cases for Privex's Python Database Wrappers (privex-db).


Testing pre-requisites
----------------------

    - Ensure you have any mandatory requirements installed (see setup.py's install_requires)
    - You should install ``pytest`` to run the tests, it works much better than standard python unittest.
    - You may wish to install any optional requirements listed in README.md for best results
    - Python 3.7 is recommended at the time of writing this. See README.md in-case this has changed.

For the best testing experience, it's recommended to install the ``dev`` extra, which includes every optional
dependency, as well as development requirements such as ``pytest`` , ``coverage`` as well as requirements for
building the documentation.


Running via PyTest
------------------

To run the tests, we strongly recommend using the ``pytest`` tool (used by default for our Travis CI)::

    # Install PyTest if you don't already have it.
    user@host: ~/privex-db $ pip3 install pytest
    
    # We recommend adding the option ``-rxXs`` which will show information about why certain tests were skipped
    # as well as info on xpass / xfail tests
    # You can add `-v` for more detailed output, just like when running the tests directly.
    user@host: ~/privex-db $ pytest -rxXs
    
    # NOTE: If you're using a virtualenv, sometimes you may encounter strange conflicts between a global install
    # of PyTest, and the virtualenv PyTest, resulting in errors related to packages not being installed.
    # A simple workaround is just to call pytest as a module from the python3 executable:
    
    user@host: ~/privex-db $ python3 -m pytest -rxXs

    
    =============================================== test session starts ===============================================
    platform darwin -- Python 3.8.0, pytest-5.3.1, py-1.8.0, pluggy-0.13.1
    cachedir: .pytest_cache
    rootdir: /home/user/privex-db, inifile: pytest.ini
    plugins: cov-2.8.1
    collected 23 items
    
    tests/test_postgres_builder.py::TestPostgresBuilder::test_all_call SKIPPED                                  [  4%]
    tests/test_postgres_builder.py::TestPostgresBuilder::test_group_call SKIPPED                                [  8%]
    tests/test_postgres_builder.py::TestPostgresBuilder::test_query_all SKIPPED                                 [ 13%]
    tests/test_postgres_builder.py::TestPostgresBuilder::test_where_call SKIPPED                                [ 34%]
    tests/test_sqlite_builder.py::TestSQLiteBuilder::test_all_call PASSED                                       [ 39%]
    tests/test_sqlite_builder.py::TestSQLiteBuilder::test_group_call PASSED                                     [ 43%]
    tests/test_sqlite_builder.py::TestSQLiteBuilder::test_query_select_col_where_group PASSED                   [ 56%]
    tests/test_sqlite_builder.py::TestSQLiteBuilder::test_query_where_first_name_last_name PASSED               [ 65%]
    tests/test_sqlite_builder.py::TestSQLiteBuilder::test_where_call PASSED                                     [ 69%]
    tests/test_sqlite_wrapper.py::TestSQLiteWrapper::test_find_user_dict_mode PASSED                            [ 73%]
    tests/test_sqlite_wrapper.py::TestSQLiteWrapper::test_insert_find_user PASSED                               [ 91%]
    tests/test_sqlite_wrapper.py::TestSQLiteWrapper::test_tables_created PASSED                                 [ 95%]
    tests/test_sqlite_wrapper.py::TestSQLiteWrapper::test_tables_drop PASSED                                    [100%]
    
    ============================================= short test summary info =============================================
    SKIPPED [1] tests/test_postgres_builder.py:132: Library 'psycopg2' is not installed...
    SKIPPED [1] tests/test_postgres_builder.py:159: Library 'psycopg2' is not installed...
    SKIPPED [1] tests/test_postgres_builder.py:78: Library 'psycopg2' is not installed...
    SKIPPED [1] tests/test_postgres_builder.py:91: Library 'psycopg2' is not installed...
    SKIPPED [1] tests/test_postgres_builder.py:116: Library 'psycopg2' is not installed...
    SKIPPED [1] tests/test_postgres_builder.py:102: Library 'psycopg2' is not installed...
    SKIPPED [1] tests/test_postgres_builder.py:84: Library 'psycopg2' is not installed...
    SKIPPED [1] tests/test_postgres_builder.py:146: Library 'psycopg2' is not installed...
    ========================================== 15 passed, 8 skipped in 0.13s ==========================================


Running individual test modules
-------------------------------

Sometimes, you just want to run only a specific test file.

Thankfully, PyTest allows you to run individual test modules like this::
    
    
    user@host: ~/privex-db $ pytest -rxXs -v tests/test_postgres_builder.py
    =============================================== test session starts ===============================================
    platform darwin -- Python 3.8.0, pytest-5.3.1, py-1.8.0, pluggy-0.13.1
    cachedir: .pytest_cache
    rootdir: /home/user/privex-db, inifile: pytest.ini
    plugins: cov-2.8.1
    collected 8 items
    
    tests/test_postgres_builder.py::TestPostgresBuilder::test_all_call PASSED                                   [ 12%]
    tests/test_postgres_builder.py::TestPostgresBuilder::test_group_call PASSED                                 [ 25%]
    tests/test_postgres_builder.py::TestPostgresBuilder::test_query_all PASSED                                  [ 37%]
    tests/test_postgres_builder.py::TestPostgresBuilder::test_query_select_col_where PASSED                     [ 50%]
    tests/test_postgres_builder.py::TestPostgresBuilder::test_query_select_col_where_group PASSED               [ 62%]
    tests/test_postgres_builder.py::TestPostgresBuilder::test_query_select_col_where_order PASSED               [ 75%]
    tests/test_postgres_builder.py::TestPostgresBuilder::test_query_where_first_name_last_name PASSED           [ 87%]
    tests/test_postgres_builder.py::TestPostgresBuilder::test_where_call PASSED                                 [100%]
    
    ================================================ 8 passed in 0.17s ================================================
    


**Copyright**::

        +===================================================+
        |                 © 2019 Privex Inc.                |
        |               https://www.privex.io               |
        +===================================================+
        |                                                   |
        |        Privex's Python Database Library           |
        |        License: X11 / MIT                         |
        |                                                   |
        |        Originally Developed by Privex Inc.        |
        |        Core Developer(s):                         |
        |                                                   |
        |          (+)  Chris (@someguy123) [Privex]        |
        |                                                   |
        +===================================================+

    Copyright 2019     Privex Inc.   ( https://www.privex.io )


"""

from tests.base import *
from tests.test_sqlite_wrapper import *
from tests.test_sqlite_builder import *
from tests.test_postgres import *

import dotenv

try:
    dotenv.read_dotenv()
except AttributeError:
    dotenv.load_dotenv()
