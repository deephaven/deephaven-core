TableHandleManager and TableHandle
==================================

TableHandleManager
------------------

:cpp:class:`TableHandleManager <deephaven::client::TableHandleManager>` is used to access existing tables in the system (e.g. via
:cpp:func:`fetchTable <deephaven::client::TableHandleManager::fetchTable>`)
or create new tables (e.g. via
:cpp:func:`emptyTable <deephaven::client::TableHandleManager::emptyTable>` or
:cpp:func:`timeTable <deephaven::client::TableHandleManager::timeTable>`).
These calls return a 
:cpp:class:`TableHandle <deephaven::client::TableHandle>`.

:cpp:class:`TableHandleManager <deephaven::client::TableHandleManager>` can also be used to access the
:cpp:class:`arrow::flight::FlightClient` for direct access to Arrow.

TableHandle
-----------

Once you have a
:cpp:class:`TableHandle <deephaven::client::TableHandle>`,
you can create derived tables via a large variety of methods, such as
:cpp:func:`where <deephaven::client::TableHandle::where>`
and
:cpp:func:`sort <deephaven::client::TableHandle::sort>`.

A simple example is:

.. code:: c++

   TableHandle mydata = manager.fetchTable("MyData");
   TableHandle filtered = t1.where("Price < 100").sort("Timestamp").tail(5);

Declarations
------------

.. doxygenclass:: deephaven::client::TableHandleManager
   :members:

.. doxygenclass:: deephaven::client::TableHandle
   :members:
