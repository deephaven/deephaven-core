TableHandleManager and TableHandle
==================================

TableHandleManager
------------------

:cpp:class:`TableHandleManager <deephaven::client::TableHandleManager>` is used to access existing tables in the system (e.g. via
:cpp:func:`FetchTable <deephaven::client::TableHandleManager::FetchTable>`)
or create new tables (e.g. via
:cpp:func:`EmptyTable <deephaven::client::TableHandleManager::EmptyTable>` or
:cpp:func:`TimeTable <deephaven::client::TableHandleManager::TimeTable>`).
These calls return a 
:cpp:class:`TableHandle <deephaven::client::TableHandle>`.

:cpp:class:`TableHandleManager <deephaven::client::TableHandleManager>` can also be used to access the
:cpp:class:`arrow::flight::FlightClient` for direct access to Arrow.

TableHandle
-----------

Once you have a
:cpp:class:`TableHandle <deephaven::client::TableHandle>`,
you can create derived tables via a large variety of methods, such as
:cpp:func:`Where <deephaven::client::TableHandle::Where>`
and
:cpp:func:`Sort <deephaven::client::TableHandle::Sort>`.

A simple example is:

.. code:: c++

   TableHandle my_data = manager.FetchTable("MyData");
   TableHandle filtered = my_data.Where("Price < 100")
       .Sort(SortPair("Timestamp"))
       .Tail(5);

Declarations
------------

.. doxygenclass:: deephaven::client::TableHandleManager
   :members:

.. doxygenclass:: deephaven::client::TableHandle
   :members:
