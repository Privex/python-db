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
from typing import Any, Iterable, Union
from typing_extensions import Protocol


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
