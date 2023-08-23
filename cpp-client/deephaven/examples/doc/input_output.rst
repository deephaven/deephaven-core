Getting Data Into and Out of Deephaven
======================================

The Deephaven system uses the
`Arrow Flight RPC <https://arrow.apache.org/docs/cpp/flight.html>`__
to move data into and out of tables. Clients that want to read or write data
will obtain Arrow ``FlightStreamReader`` or ``FlightStreamWriter``, as
appropriate, and read or write their to that object.

Tables are created and populated using Arrow Flight's ``DoPut`` functionality.
``DoPut`` is powerful and flexible, but it may be daunting for first-time
users. To help users get started, we we provide a helper class called
:cpp:class:`TableMaker <deephaven::client::utility::TableMaker>`.
This class provides a simpler interface for creating small tables.
Likewise, we provide a method for streaming a table to a ``std::ostream``.

.. toctree::

  create_table_with_tablemaker
  create_table_with_arrow_flight
  read_data_with_arrow_flight
