Column Sources
==============

Description
-----------

A
:cpp:class:`ColumnSource <deephaven::dhcore::column::ColumnSource>` 
is an abstract class representing a Deephaven column. It represents a read-only view on that
column. There is a derived class,
:cpp:class:`MutableColumnSource <deephaven::dhcore::column::MutableColumnSource>`,
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
hierarchy is further divided into the generic type
:cpp:class:`GenericColumnSource <deephaven::dhcore::column::GenericColumnSource>`
and its mutable counterpart
:cpp:class:`MutableGenericColumnSource <deephaven::dhcore::column::MutableGenericColumnSource>`.
These types are generic on the element type they contain.

Supported types
---------------

The following types are supported

=======================  =========================
Java type                C++ type
=======================  =========================
byte                     int8_t
short                    int16_t
int                      int32_t
long                     int64_t
float                    float
double                   double
boolean                  bool
char                     char16_t
java.lang.String         std::string
java.time.ZonedDateTime  :cpp:class:`DateTime <deephaven::dhcore::DateTime>`
java.time.LocalDate      :cpp:class:`LocalDate <deephaven::dhcore::LocalDate>`
java.time.LocalTime      :cpp:class:`LocalTime <deephaven::dhcore::LocalTime>`
=======================  =========================

as well as lists of the above. Lists are stored in a custom container; the element type is
std::shared_ptr<:cpp:type:`ContainerBase <deephaven::dhcore::container::ContainerBase>`>

:cpp:type:`ContainerBase <deephaven::dhcore::container::ContainerBase>`
is described :doc:`in the section on Containers <containers>`.

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
* :cpp:type:`LocalDateColumnSource <deephaven::dhcore::column::LocalDateColumnSource>`
* :cpp:type:`LocalTimeColumnSource <deephaven::dhcore::column::LocalTimeColumnSource>`
* :cpp:type:`ContainerBaseColumnSource <deephaven::dhcore::column::ContainerBaseColumnSource>`

Declarations
------------

.. doxygenclass:: deephaven::dhcore::column::ColumnSource
   :members:

.. doxygenclass:: deephaven::dhcore::column::MutableColumnSource
   :members:

.. doxygenclass:: deephaven::dhcore::column::GenericColumnSource
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

.. doxygentypedef:: deephaven::dhcore::column::LocalDateColumnSource

.. doxygentypedef:: deephaven::dhcore::column::LocalTimeColumnSource

.. doxygentypedef:: deephaven::dhcore::column::ContainerBaseColumnSource

Utility Declarations
--------------------

.. doxygenclass:: deephaven::dhcore::column::ColumnSourceVisitor
   :members:
