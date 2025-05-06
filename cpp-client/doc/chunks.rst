Chunks
======

Description
-----------

:cpp:class:`Chunk <deephaven::dhcore::chunk::Chunk>`
is the abstract base class representing a simple typed data buffer.
These buffers are used to pass data to and from the library, e.g. as arguments to
:cpp:func:`FillChunk <deephaven::dhcore::column::ColumnSource::FillChunk>`
or
:cpp:func:`FillFromChunk <deephaven::dhcore::column::MutableColumnSource::FillFromChunk>`.

The concrete implementing classes are defined by the templated class
:cpp:class:`GenericChunk <deephaven::dhcore::chunk::GenericChunk>`.
For convenience we provide typedefs which instantiate
:cpp:class:`GenericChunk <deephaven::dhcore::chunk::GenericChunk>`
on all the Deephaven types:
:cpp:type:`Int8Chunk <deephaven::dhcore::chunk::Int8Chunk>`,
:cpp:type:`Int16Chunk <deephaven::dhcore::chunk::Int16Chunk>`,
:cpp:type:`Int32Chunk <deephaven::dhcore::chunk::Int32Chunk>`,
:cpp:type:`Int64Chunk <deephaven::dhcore::chunk::Int64Chunk>`,
:cpp:type:`FloatChunk <deephaven::dhcore::chunk::FloatChunk>`,
:cpp:type:`DoubleChunk <deephaven::dhcore::chunk::DoubleChunk>`,
:cpp:type:`BooleanChunk <deephaven::dhcore::chunk::BooleanChunk>`,
:cpp:type:`StringChunk <deephaven::dhcore::chunk::StringChunk>`,
:cpp:type:`DateTimeChunk <deephaven::dhcore::chunk::DateTimeChunk>`.
:cpp:type:`DateTimeChunk <deephaven::dhcore::chunk::LocalDateChunk>`.
:cpp:type:`DateTimeChunk <deephaven::dhcore::chunk::LocalTimeChunk>`. and
:cpp:type:`DateTimeChunk <deephaven::dhcore::chunk::ContainerBaseChunk>`.

:cpp:class:`GenericChunk <deephaven::dhcore::chunk::GenericChunk>`
also supports the methods
:cpp:func:`Take <deephaven::dhcore::chunk::GenericChunk::Take>` and
:cpp:func:`Drop <deephaven::dhcore::chunk::GenericChunk::Drop>` to take slices of the
:cpp:class:`GenericChunk <deephaven::dhcore::chunk::GenericChunk>`.

AnyChunk
--------

The
:cpp:class:`AnyChunk <deephaven::dhcore::chunk::AnyChunk>`
class is a variant value type that can hold one of the concrete Chunk types described above.
:cpp:class:`AnyChunk <deephaven::dhcore::chunk::AnyChunk>` is useful in certain limited cases
where a factory method needs to create a
:cpp:class:`Chunk <deephaven::dhcore::chunk::Chunk>`
having a dynamically-determined type, not known at compile time. Of course this could also be
accomplished by returning a heap-allocated pointer to a
:cpp:class:`Chunk <deephaven::dhcore::chunk::Chunk>`.
The rationale for using the variant approach rather than the
heap-allocated object approach is for the sake of simplicity and efficiency when using these
small objects. One example method that returns an
:cpp:class:`AnyChunk <deephaven::dhcore::chunk::AnyChunk>`
is
:cpp:func:`CreateChunkFor <deephaven::dhcore::chunk::ChunkMaker::CreateChunkFor>`,
which creates a
:cpp:class:`Chunk <deephaven::dhcore::chunk::Chunk>`
with a type appropriate to the passed-in
:cpp:class:`ColumnSource <deephaven::dhcore::column::ColumnSource>`,
and wraps that dynamicaly-determined Chunk in an
:cpp:class:`AnyChunk <deephaven::dhcore::chunk::AnyChunk>` value.

Chunk Declarations
------------------

.. doxygenclass:: deephaven::dhcore::chunk::Chunk
   :members:

.. doxygenclass:: deephaven::dhcore::chunk::GenericChunk
   :members:

.. doxygentypedef:: deephaven::dhcore::chunk::Int8Chunk

.. doxygentypedef:: deephaven::dhcore::chunk::Int16Chunk

.. doxygentypedef:: deephaven::dhcore::chunk::Int32Chunk

.. doxygentypedef:: deephaven::dhcore::chunk::Int64Chunk

.. doxygentypedef:: deephaven::dhcore::chunk::FloatChunk

.. doxygentypedef:: deephaven::dhcore::chunk::DoubleChunk

.. doxygentypedef:: deephaven::dhcore::chunk::BooleanChunk

.. doxygentypedef:: deephaven::dhcore::chunk::StringChunk

.. doxygentypedef:: deephaven::dhcore::chunk::DateTimeChunk

.. doxygentypedef:: deephaven::dhcore::chunk::LocalDateChunk

.. doxygentypedef:: deephaven::dhcore::chunk::LocalTimeChunk

.. doxygentypedef:: deephaven::dhcore::chunk::ContainerBaseChunk

Utility Declarations
--------------------

.. doxygenclass:: deephaven::dhcore::chunk::AnyChunk
   :members:

.. doxygenclass:: deephaven::dhcore::chunk::ChunkVisitor
   :members:

.. doxygenclass:: deephaven::dhcore::chunk::ChunkMaker
   :members:
