Fluent classes representing columns
===================================

Typically these are created by a
:cpp:func:`TableHandle::select <deephaven::client::TableHandle::getCol>` or
:cpp:func:`TableHandle::select <deephaven::client::TableHandle::getCols>`
method. For example

.. code:: c++

  auto [symbol, price] = table.getCols<StrCol, NumCol>("Symbol", "Price");

Leaf classes
------------

.. doxygenclass:: deephaven::client::NumCol
   :members:

.. doxygenclass:: deephaven::client::StrCol
   :members:

.. doxygenclass:: deephaven::client::DateTimeCol
   :members:      

.. 
    TODO(kosak):
    .. doxygenclass:: deephaven::client::BoolCol
    :members:

TODO(kosak) - BoolCol

Intermediate classes
--------------------

These classes are typically not specified directly by client code. Instead
they are used by library methods to indicate the type of column they work with.

.. doxygenclass:: deephaven::client::SelectColumn
   :members:

.. doxygenclass:: deephaven::client::MatchWithColumn
   :members:

.. doxygenclass:: deephaven::client::AssignedColumn
   :members:

.. doxygenclass:: deephaven::client::Column
   :members:
