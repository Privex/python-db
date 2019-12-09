"""
This module holds newly defined types which are used across the module, such as :class:`.GenericCursor` and
:class:`.GenericConnection`

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


"""
from typing import Any, Iterable, Union, Coroutine, Type, Optional
from typing_extensions import Protocol

CoroNone = Type[Coroutine[Any, Any, None]]

TUP_DICT = Union[Iterable[dict], Iterable[tuple]]
TUPDICT_OPT = Optional[Union[dict, tuple]]

# The below types are for @awaitable functions, which might either synchronously return their value,
# or return a coroutine which asynchronously returns the value.
STR_CORO = Union[str, Coroutine[Any, Any, str]]
INT_CORO = Union[int, Coroutine[Any, Any, int]]
BOOL_CORO = Union[bool, Coroutine[Any, Any, bool]]
DICT_CORO = Union[dict, Coroutine[Any, Any, dict]]
ITER_CORO = Union[Iterable, Coroutine[Any, Any, Iterable]]
TUPD_CORO = Union[TUP_DICT, Coroutine[Any, Any, TUP_DICT]]
TUPD_OPT_CORO = Union[TUPDICT_OPT, Coroutine[Any, Any, TUPDICT_OPT]]
ANY_CORO = Union[Any, Coroutine[Any, Any, Any]]


class GenericCursor(Protocol):
    """
    This is a :class:`typing_extensions.Protocol` which represents any database Cursor object which follows
    the Python DB API (PEP 249).
    """
    rowcount: int
    lastrowid: Any
    connection: Any
    description: Any
    
    def close(self, *args, **kwargs): pass
    
    def execute(self, query: str, params: Iterable = None, *args, **kwargs) -> Any: pass

    def executemany(self, query: str, params: Iterable = None, *args, **kwargs) -> Any: pass
    
    def fetchone(self, *args, **kwargs) -> Union[tuple, list, dict, set]: pass

    def fetchall(self, *args, **kwargs) -> Iterable: pass

    def fetchmany(self, *args, **kwargs) -> Iterable: pass


class GenericAsyncCursor(Protocol):
    
    async def close(self, *args, **kwargs): pass
    
    async def execute(self, query: str, params: Iterable = None, *args, **kwargs) -> Any: pass
    
    async def executemany(self, query: str, params: Iterable = None, *args, **kwargs) -> Any: pass
    
    async def fetchone(self, *args, **kwargs) -> Union[tuple, list, dict, set]: pass
    
    async def fetchall(self, *args, **kwargs) -> Iterable: pass
    
    async def fetchmany(self, *args, **kwargs) -> Iterable: pass


class GenericConnection(Protocol):
    """
    This is a :class:`typing_extensions.Protocol` which represents any database Connection object which follows
    the Python DB API (PEP 249).
    """
    def __init__(self, *args, **kwargs): pass
    
    def cursor(self, *args, **kwargs) -> GenericCursor: pass

    def commit(self, *args, **kwargs): pass

    def rollback(self, *args, **kwargs): pass

    def close(self, *args, **kwargs): pass


class GenericAsyncConnection(Protocol):
    def __init__(self, *args, **kwargs): pass
    
    async def cursor(self, *args, **kwargs) -> GenericAsyncCursor: pass
    
    async def commit(self, *args, **kwargs): pass
    
    async def rollback(self, *args, **kwargs): pass
    
    async def close(self, *args, **kwargs): pass

