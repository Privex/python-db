.. _Privex Python Database Wrappers documentation:



Privex Python Database Wrappers (privex-db) documentation
=================================================

.. image:: https://www.privex.io/static/assets/svg/brand_text_nofont.svg
   :target: https://www.privex.io/
   :width: 400px
   :height: 400px
   :alt: Privex Logo
   :align: center

Welcome to the documentation for Privex's `Python Database Wrappers`_ - lightweight classes and functions to ease
managing and interacting with various relation database systems (RDBMS's), including SQLite3 and PostgreSQL.

This documentation is automatically kept up to date by ReadTheDocs, as it is automatically re-built each time
a new commit is pushed to the `Github Project`_ 

.. _Django Lock Manager: https://github.com/Privex/python-db
.. _Github Project: https://github.com/Privex/python-db

.. contents::


Quick install
-------------

**Installing with** `Pipenv`_ **(recommended)**

.. code-block:: bash

    pipenv install privex-db


**Installing with standard** ``pip3``

.. code-block:: bash

    pip3 install privex-db



.. _Pipenv: https://pipenv.kennethreitz.org/en/latest/





All Documentation
=================

.. toctree::
   :maxdepth: 8
   :caption: Main:

   self
   install
   examples


.. toctree::
   :maxdepth: 3
   :caption: Code Documentation:

   privex_db/index


.. toctree::
   :caption: Unit Testing

   privex_db/tests


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
