Top-level class: Client
=======================

Description
-----------

The
:cpp:class:`Client <deephaven::client::Client>`
class is the main entry point to the Deephaven Client API. Use
:cpp:func:`connect <deephaven::client::Client::connect>`
to connect to a Deephaven instance, then call
:cpp:func:`getManager <deephaven::client::Client::getManager>`
to get a
:cpp:class:`TableHandleManager <deephaven::client::TableHandleManager>`

Example:

.. code:: c++

   const char *server = "localhost:10000";
   auto client = Client::connect(server);
   auto manager = client.getManager();
   auto mydata = manager.fetchTable("MyData");
   auto filtered = t1.where("Price < 100").sort("Timestamp").tail(5);

Declarations
------------

.. doxygenclass:: deephaven::client::Client
   :members:
