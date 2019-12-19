#!/usr/bin/env python3
"""
+===================================================+
|                 © 2019 Privex Inc.                |
|               https://www.privex.io               |
+===================================================+
|                                                   |
|        Django Database Lock Manager               |
|        License: X11/MIT                           |
|                                                   |
|        Core Developer(s):                         |
|                                                   |
|          (+)  Chris (@someguy123) [Privex]        |
|                                                   |
+===================================================+

Django Database Lock Manager - Easy to use lock system using your Django app's database
Copyright (c) 2019    Privex Inc. ( https://www.privex.io )

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of
the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name(s) of the above copyright holders shall not be used in advertising or
otherwise to promote the sale, use or other dealings in this Software without prior written authorization.
"""
import warnings

from setuptools import setup, find_packages
import os
from privex import db

with open("README.md", "r") as fh:
    long_description = fh.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

extra_commands = {}

try:
    # noinspection PyUnresolvedReferences
    from privex.helpers import settings

    settings.VERSION_FILE = os.path.join(BASE_DIR, 'privex', 'db', '__init__.py')
    import privex.helpers.setuppy.commands
    
    extra_commands['extras'] = privex.helpers.setuppy.commands.ExtrasCommand
    extra_commands['bump'] = privex.helpers.setuppy.commands.BumpCommand
except (ImportError, AttributeError) as e:
    warnings.warn('Failed to import privex.helpers.setuppy.commands - the command "bump" may not work.')
    warnings.warn(f'Error Reason: {type(e)} - {str(e)}')


setup(
    name='privex_db',

    version=db.VERSION,

    description="Privex's Python Database helpers and wrappers (mini ORM)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Privex/python-db",
    author='Chris (Someguy123) @ Privex',
    author_email='chris@privex.io',

    license='MIT',
    install_requires=[
        'privex-helpers>=2.6.0',
        'aiosqlite',
        'async-property',
        'python-dateutil',
        'pytz',
        'typing-extensions',
    ],
    packages=find_packages(exclude=['tests', 'test.*']),
    cmdclass=extra_commands,
    extras_require={
        'postgres': ['psycopg2'],
        'async': ['nest_asyncio'],
        'nest': ['nest_asyncio'],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
