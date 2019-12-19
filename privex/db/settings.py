from privex.helpers import DictObject

USE_NEST_ASYNCIO = True
"""

We use the nest_asyncio library to reduce conflicts with other library's event loops, however, some AsyncIO libraries don't play nicely
when nest_asyncio has been setup.

To deal with this issue, instead of directly importing nest_asyncio and calling ``apply``, we dynamically load nest_asyncio during the
constructor of certain AsyncIO classes where it may be helpful, using the helper function :func:`.setup_nest_async`.

This ensures that if you have issues caused by nest_asyncio, you have the ability to disable the use of it within this library by setting
this to False (as long as you haven't yet initialised any of the AsyncIO database classes) like so::

    >>> import privex.db.settings
    >>>
    >>> privex.db.settings.USE_NEST_ASYNCIO = False


"""


STATE = DictObject(
    loaded=DictObject(
        nest_asyncio=False
    )
)
"""
Not really a setting, but the settings module is a safe place to put this "shared application state" dictionary,
since the settings module by nature cannot import any modules from the library.
"""


