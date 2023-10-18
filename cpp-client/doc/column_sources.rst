Column Sources
==============

Description
-----------

A
:cpp:class:`ColumnSource <deephaven::dhcore::column::ColumnSource>` 
is an abstract class representing a Deephaven column. It represents a read-only view on that
column. There is a derived class,
:cpp:class:`MutableColumnSource <deephaven::dhcore::column::MutableColumnSource>`
which provides a writable interface.

You can access the data in a
:cpp:class:`ColumnSource <deephaven::dhcore::column::ColumnSource>`
via its
:cpp:func:`FillChunk <deephaven::dhcore::column::ColumnSource::FillChunk>`
and
:cpp:func:`FillChunkUnordered <deephaven::dhcore::column::ColumnSource::FillChunkUnordered>`
methods. Likewise, you can store data into a
:cpp:class:`MutableColumnSource <deephaven::dhcore::column::MutableColumnSource>`
via its
:cpp:func:`FillFromChunk <deephaven::dhcore::column::MutableColumnSource::FillFromChunk>`
and
:cpp:func:`FillFromChunkUnordered <deephaven::dhcore::column::MutableColumnSource::FillFromChunkUnordered>`
methods. These methods provide "bulk transfer" of data into and out of a
:cpp:class:`ColumnSource <deephaven::dhcore::column::ColumnSource>`.
We do not provide any methods to access single elements of a
:cpp:class:`ColumnSource <deephaven::dhcore::column::ColumnSource>`
because we want to encourage callers to use the more efficient bulk transfer methods.

The
:cpp:class:`ColumnSource <deephaven::dhcore::column::ColumnSource>`
hierarchy is further divided into two parts:
:cpp:class:`NumericColumnSource <deephaven::dhcore::column::NumericColumnSource>`
and
:cpp:class:`GenericColumnSource <deephaven::dhcore::column::GenericColumnSource>`
(and their mutable counterparts
:cpp:class:`MutableNumericColumnSource <deephaven::dhcore::column::MutableNumericColumnSource>`
and
:cpp:class:`MutableGenericColumnSource <deephaven::dhcore::column::MutableGenericColumnSource>`).

:cpp:class:`ColumnSource <deephaven::dhcore::column::NumericColumnSource>`
is for representing
columns containing the numeric Deephaven types (``int8_t``, ``int16_t``, ``int32_t``,
``int64_t``, ``float``, ``double``), whereas
:cpp:class:`ColumnSource <deephaven::dhcore::column::GenericColumnSource>`
is for representing
the remaining Deephaven types (``bool``, ``std::string``, and
:cpp:class:`DateTime <deephaven::dhcore::DateTime>`).

For these types we have a set of convenience typedefs:

* :cpp:type:`Int8ColumnSource <deephaven::dhcore::column::Int8ColumnSource>`
* :cpp:type:`Int16ColumnSource <deephaven::dhcore::column::Int16ColumnSource>`
* :cpp:type:`Int32ColumnSource <deephaven::dhcore::column::Int32ColumnSource>`
* :cpp:type:`Int64ColumnSource <deephaven::dhcore::column::Int64ColumnSource>`
* :cpp:type:`FloatColumnSource <deephaven::dhcore::column::FloatColumnSource>`
* :cpp:type:`DoubleColumnSource <deephaven::dhcore::column::DoubleColumnSource>`
* :cpp:type:`BooleanColumnSource <deephaven::dhcore::column::BooleanColumnSource>`
* :cpp:type:`StringColumnSource <deephaven::dhcore::column::StringColumnSource>`
* :cpp:type:`DateTimeColumnSource <deephaven::dhcore::column::DateTimeColumnSource>`

Declarations
------------

.. doxygenclass:: deephaven::dhcore::column::ColumnSource
   :members:

.. doxygenclass:: deephaven::dhcore::column::MutableColumnSource
   :members:

.. doxygenclass:: deephaven::dhcore::column::NumericColumnSource
   :members:

.. doxygenclass:: deephaven::dhcore::column::GenericColumnSource
   :members:

.. doxygenclass:: deephaven::dhcore::column::MutableNumericColumnSource
   :members:

.. doxygenclass:: deephaven::dhcore::column::MutableGenericColumnSource
   :members:

.. doxygentypedef:: deephaven::dhcore::column::Int8ColumnSource

.. doxygentypedef:: deephaven::dhcore::column::Int16ColumnSource

.. doxygentypedef:: deephaven::dhcore::column::Int32ColumnSource

.. doxygentypedef:: deephaven::dhcore::column::Int64ColumnSource

.. doxygentypedef:: deephaven::dhcore::column::FloatColumnSource

.. doxygentypedef:: deephaven::dhcore::column::DoubleColumnSource

.. doxygentypedef:: deephaven::dhcore::column::BooleanColumnSource

.. doxygentypedef:: deephaven::dhcore::column::StringColumnSource

.. doxygentypedef:: deephaven::dhcore::column::DateTimeColumnSource

Utility Declarations
--------------------

.. doxygenclass:: deephaven::dhcore::column::ColumnSourceVisitor
   :members:
