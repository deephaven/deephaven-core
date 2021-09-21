Fluent classes representing columns
===================================

Typically these are created by a
:cpp:func:`TableHandle::select <deephaven::client::highlevel::TableHandle::getCol>` or
:cpp:func:`TableHandle::select <deephaven::client::highlevel::TableHandle::getCols>`
method. For example

.. code:: c++

  auto [symbol, price] = table.getCols<StrCol, NumCol>("Symbol", "Price");

Leaf classes
------------

.. doxygenclass:: deephaven::client::highlevel::NumCol
   :members:

.. doxygenclass:: deephaven::client::highlevel::StrCol
   :members:

.. doxygenclass:: deephaven::client::highlevel::DateTimeCol
   :members:      

.. doxygenclass:: deephaven::client::highlevel::BoolCol
   :members:

TODO(kosak) - BoolCol

Intermediate classes
--------------------

These classes are typically not specified directly by client code. Instead
they are used by library methods to indicate the type of column they work with.

.. doxygenclass:: deephaven::client::highlevel::SelectColumn
   :members:

.. doxygenclass:: deephaven::client::highlevel::MatchWithColumn
   :members:

.. doxygenclass:: deephaven::client::highlevel::AssignedColumn
   :members:

.. doxygenclass:: deephaven::client::highlevel::Column
   :members:
