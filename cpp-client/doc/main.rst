Top-level class: Client
=======================

Description
-----------

The
:cpp:class:`Client <deephaven::client::Client>`
class is the main entry point to the Deephaven Client API. Use
:cpp:func:`Connect <deephaven::client::Client::Connect>`
to connect to a Deephaven instance, then call
:cpp:func:`GetManager <deephaven::client::Client::GetManager>`
to get a
:cpp:class:`TableHandleManager <deephaven::client::TableHandleManager>`

Example:

.. code:: c++

  const char *server = "localhost:10000";
  auto client = Client::Connect(server);
  auto manager = client.GetManager();
  auto my_data = manager.FetchTable("MyData");
  auto filtered = my_data.Where("Price < 100")
      .Sort(SortPair("Timestamp"))
      .Tail(5);

Declarations
------------

.. doxygenclass:: deephaven::client::Client
   :members:

.. doxygenclass:: deephaven::client::ClientOptions
   :members:
