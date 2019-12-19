"""
Privex's Python Database Library - https://github.com/privex/privex-db

X11 / MIT License

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

    Copyright (c) 2019     Privex Inc.   ( https://www.privex.io )

    Permission is hereby granted, free of charge, to any person obtaining a copy of
    this software and associated documentation files (the "Software"), to deal in
    the Software without restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
    Software, and to permit persons to whom the Software is furnished to do so,
    subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
    INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
    PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
    OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
    SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""

import logging
import warnings
from privex.db.base import GenericDBWrapper, CursorManager
from privex.db.types import GenericCursor, GenericConnection
from privex.db.query.base import BaseQueryBuilder, QueryMode


__all__ = [
    'GenericDBWrapper', 'CursorManager', 'GenericCursor', 'GenericConnection', 'BaseQueryBuilder', 'QueryMode',
    'name', 'VERSION',
]

log = logging.getLogger(__name__)


try:
    from privex.db.query.postgres import PostgresQueryBuilder
    __all__ += ['PostgresQueryBuilder']
except ImportError:
    log.debug("Failed to import privex.db.query.postgres (missing psycopg2?)")

try:
    from privex.db.postgres import PostgresWrapper
    __all__ += ['PostgresWrapper']
except ImportError:
    log.debug("Failed to import privex.db.postgres (missing psycopg2?)")


try:
    from privex.db.query.sqlite import SqliteQueryBuilder
    __all__ += ['SqliteQueryBuilder']
except ImportError:
    log.warning("Failed to import privex.db.query.sqlite (missing Python SQLite API?)")

try:
    from privex.db.sqlite import SqliteWrapper
    __all__ += ['SqliteWrapper']
except ImportError:
    log.warning("Failed to import privex.db.sqlite.SqliteWrapper (missing Python SQLite API?)")

try:
    from privex.db.sqlite import SqliteAsyncWrapper
    from privex.db.query.asyncx import SqliteAsyncQueryBuilder
    
    __all__ += ['SqliteAsyncWrapper', 'SqliteAsyncQueryBuilder']
except ImportError:
    log.debug(
        "Failed to import privex.db.sqlite.SqliteAsyncWrapper and/or privex.db.query.asyncx.SqliteAsyncQueryBuilder "
        "(missing aiosqlite library?)"
    )


def _setup_logging(level=logging.WARNING):
    """
    Set up logging for the entire module ``privex.db`` . Since this is a package, we don't add any
    console or file logging handlers, we purely just set our minimum logging level to WARNING to avoid
    spamming the logs of any application importing it.
    """
    try:
        from privex.loghelper import LogHelper
        lh = LogHelper(__name__, level=level)
        return lh.get_logger()
    except ImportError:
        warnings.warn(f'{__name__} failed to import privex.loghelper. Logging may not work as expected.')
        lh = logging.getLogger(__name__)
        lh.setLevel(logging.WARNING)
        return log


log = _setup_logging()
name = 'db'

VERSION = '0.9.2'



