Deephaven C++ Client
====================

The Deephaven C++ Client Library provides a high-level interface to Deephaven
functionality. The library...

* Provides a "fluent" syntax which provides a more convenient and less
  error-prone way of expressing expressions and conditionals, with much of the
  syntax checking being done at compile time.
* Performs operations asynchronously, automatically managing dependencies
  between operations, without burdening the caller with managing callback
  state.
* Provides a shared ownership model so that objects like
  :cpp:class:`TableHandle <deephaven::client::TableHandle>`
  can be manipulated like value types (e.g. freely copied). Their corresponding
  server resources will be cleaned up when last one goes out of scope.

Contents
========

.. toctree::
   :maxdepth: 2
 
   getting_started
   resource_mgmt
   input_output
   fluent
