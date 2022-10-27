Column Sources
==============

Description
-----------

A
:cpp:class:`ColumnSource <deephaven::client::column::ColumnSource>` 
is an abstract class representing a Deephaven column. It represents a read-only view on that
column. There is a derived class,
:cpp:class:`MutableColumnSource <deephaven::client::column::MutableColumnSource>`
which provides a writable interface.

You can access the data in a
:cpp:class:`ColumnSource <deephaven::client::column::ColumnSource>`
via its
:cpp:func:`fillChunk <deephaven::client::column::ColumnSource::fillChunk>`
and
:cpp:func:`fillChunkUnordered <deephaven::client::column::ColumnSource::fillChunkUnordered>`
methods. Likewise, you can store data into a
:cpp:class:`MutableColumnSource <deephaven::client::column::MutableColumnSource>`
via its
:cpp:func:`fillFromChunk <deephaven::client::column::MutableColumnSource::fillFromChunk>`
and
:cpp:func:`fillFromChunkUnordered <deephaven::client::column::MutableColumnSource::fillFromChunkUnordered>`
methods. These methods provide "bulk transfer" of data into and out of a
:cpp:class:`ColumnSource <deephaven::client::column::ColumnSource>`.
We do not provide any methods to access single elements of a
:cpp:class:`ColumnSource <deephaven::client::column::ColumnSource>`
because we want to encourage callers to use the more efficient bulk transfer methods.

The
:cpp:class:`ColumnSource <deephaven::client::column::ColumnSource>`
hierarchy is further divided into two parts:
:cpp:class:`NumericColumnSource <deephaven::client::column::NumericColumnSource>`
and
:cpp:class:`GenericColumnSource <deephaven::client::column::GenericColumnSource>`
(and their mutable counterparts
:cpp:class:`MutableNumericColumnSource <deephaven::client::column::MutableNumericColumnSource>`
and
:cpp:class:`MutableGenericColumnSource <deephaven::client::column::MutableGenericColumnSource>`).

:cpp:class:`ColumnSource <deephaven::client::column::NumericColumnSource>`
is for representing
columns containing the numeric Deephaven types (``int8_t``, ``int16_t``, ``int32_t``,
``int64_t``, ``float``, ``double``), whereas
:cpp:class:`ColumnSource <deephaven::client::column::GenericColumnSource>`
is for representing
the remaining Deephaven types (``bool``, ``std::string``, and
:cpp:class:`DateTime <deephaven::client::DateTime>`).

For these types we have a set of convenience typedefs:

* :cpp:type:`Int8ColumnSource <deephaven::client::column::Int8ColumnSource>`
* :cpp:type:`Int16ColumnSource <deephaven::client::column::Int16ColumnSource>`
* :cpp:type:`Int32ColumnSource <deephaven::client::column::Int32ColumnSource>`
* :cpp:type:`Int64ColumnSource <deephaven::client::column::Int64ColumnSource>`
* :cpp:type:`FloatColumnSource <deephaven::client::column::FloatColumnSource>`
* :cpp:type:`DoubleColumnSource <deephaven::client::column::DoubleColumnSource>`
* :cpp:type:`BooleanColumnSource <deephaven::client::column::BooleanColumnSource>`
* :cpp:type:`StringColumnSource <deephaven::client::column::StringColumnSource>`
* :cpp:type:`DateTimeColumnSource <deephaven::client::column::DateTimeColumnSource>`

Declarations
------------

.. doxygenclass:: deephaven::client::column::ColumnSource
   :members:

.. doxygenclass:: deephaven::client::column::MutableColumnSource
   :members:

.. doxygenclass:: deephaven::client::column::NumericColumnSource
   :members:

.. doxygenclass:: deephaven::client::column::GenericColumnSource
   :members:

.. doxygenclass:: deephaven::client::column::MutableNumericColumnSource
   :members:

.. doxygenclass:: deephaven::client::column::MutableGenericColumnSource
   :members:

.. doxygentypedef:: deephaven::client::column::Int8ColumnSource

.. doxygentypedef:: deephaven::client::column::Int16ColumnSource

.. doxygentypedef:: deephaven::client::column::Int32ColumnSource

.. doxygentypedef:: deephaven::client::column::Int64ColumnSource

.. doxygentypedef:: deephaven::client::column::FloatColumnSource

.. doxygentypedef:: deephaven::client::column::DoubleColumnSource

.. doxygentypedef:: deephaven::client::column::BooleanColumnSource

.. doxygentypedef:: deephaven::client::column::StringColumnSource

.. doxygentypedef:: deephaven::client::column::DateTimeColumnSource

Utility Declarations
--------------------

.. doxygenclass:: deephaven::client::column::ColumnSourceVisitor
   :members:
