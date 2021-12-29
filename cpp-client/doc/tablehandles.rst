TableHandleManager and TableHandle
==================================

:cpp:class:`TableHandleManager <deephaven::client::highlevel::TableHandleManager>`
is one of two ways to get
:cpp:class:`TableHandle <deephaven::client::highlevel::TableHandle>` resources
in the system.

:cpp:class:`TableHandleManager <deephaven::client::highlevel::TableHandleManager>` is used to access existing tables in the system (e.g. via
:cpp:func:`fetchTable <deephaven::client::highlevel::TableHandleManager::fetchTable>`)
or create new tables (e.g. via
:cpp:func:`emptyTable <deephaven::client::highlevel::TableHandleManager::emptyTable>` or
:cpp:func:`timeTable <deephaven::client::highlevel::TableHandleManager::timeTable>`).
It is also that place that (in a future version) you can set attributes that
affect a related group of tables, such as whether they are resolved
synchronously or asynchronously.

On the other hand, the methods on
:cpp:class:`TableHandle <deephaven::client::highlevel::TableHandle>`
are used to create tables derived from other tables.
Some examples are
:cpp:func:`where <deephaven::client::highlevel::TableHandle::where>` and
:cpp:func:`sort <deephaven::client::highlevel::TableHandle::sort>`).

These are used to create tables derived from other tables. A typical pattern
might be

.. code:: c++

   TableHandle t1 = ...;
   TableHandle t2 = t1.where(...).sort(...).tail(5);

.. doxygenclass:: deephaven::client::highlevel::TableHandleManager
   :members:

.. doxygenclass:: deephaven::client::highlevel::TableHandle
   :members:

