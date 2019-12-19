import logging
from privex.db.query.base import BaseQueryBuilder, QueryMode

log = logging.getLogger(__name__)

__all__ = ['BaseQueryBuilder', 'QueryMode']

try:
    from privex.db.query.postgres import PostgresQueryBuilder
    __all__ += ['PostgresQueryBuilder']
except ImportError:
    log.debug("Failed to import privex.db.query.postgres (missing psycopg2?)")

try:
    from privex.db.query.sqlite import SqliteQueryBuilder
    from privex.db.query.asyncx.sqlite import SqliteAsyncQueryBuilder

    __all__ += ['SqliteQueryBuilder', 'SqliteAsyncQueryBuilder']
except ImportError:
    log.debug("Failed to import privex.db.query.sqlite (missing Python SQLite API?)")


