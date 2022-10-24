Deephaven C++ Client API Reference
==================================

Introduction
------------

The Deephaven C++ Client is used to manipulate tables and access data running on a remote
Deephaven server from native C++. This document provides the major abstractions and entry
points provided in C++. To start, create a
:cpp:class:`Client <deephaven::client::Client>`
with the
:cpp:func:`connect <deephaven::client::Client::connect>`
factory method. To get started, see :doc:`main`.


Table of Contents
-----------------

.. toctree::
   :maxdepth: 2

   main
   tablehandles
   ticking
   row_sequences
   column_sources
   chunks
   fluent
   types
   utility

Index
-----

You may also want to consult the :ref:`Index <genindex>`.
