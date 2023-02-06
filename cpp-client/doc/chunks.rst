Chunks
======

Description
-----------

:cpp:class:`Chunk <deephaven::client::chunk::Chunk>`
is the abstract base class representing a simple typed data buffer.
These buffers are used to pass data to and from the library, e.g. as arguments to
:cpp:func:`fillChunk <deephaven::client::column::ColumnSource::fillChunk>`
or
:cpp:func:`fillFromChunk <deephaven::client::column::MutableColumnSource::fillFromChunk>`.

The concrete implementing classes are defined by the templated class
:cpp:class:`GenericChunk <deephaven::client::chunk::GenericChunk>`.
For convenience we provide typedefs which instantiate
:cpp:class:`GenericChunk <deephaven::client::chunk::GenericChunk>`
on all the Deephaven types:
:cpp:type:`Int8Chunk <deephaven::client::chunk::BooleanChunk>`,
:cpp:type:`Int16Chunk <deephaven::client::chunk::BooleanChunk>`,
:cpp:type:`Int32Chunk <deephaven::client::chunk::BooleanChunk>`,
:cpp:type:`Int64Chunk <deephaven::client::chunk::BooleanChunk>`,
:cpp:type:`FloatChunk <deephaven::client::chunk::BooleanChunk>`,
:cpp:type:`DoubleChunk <deephaven::client::chunk::BooleanChunk>`,
:cpp:type:`BooleanChunk <deephaven::client::chunk::BooleanChunk>`,
:cpp:type:`StringChunk <deephaven::client::chunk::BooleanChunk>`, and
:cpp:type:`DateTimeChunk <deephaven::client::chunk::BooleanChunk>`.

:cpp:class:`GenericChunk <deephaven::client::chunk::GenericChunk>`
also supports the methods
:cpp:func:`take <deephaven::client::chunk::GenericChunk::take>` and
:cpp:func:`drop <deephaven::client::chunk::GenericChunk::drop>` to take slices of the
:cpp:class:`GenericChunk <deephaven::client::chunk::GenericChunk>`.

AnyChunk
--------

The
:cpp:class:`AnyChunk <deephaven::client::chunk::AnyChunk>`
class is a variant value type that can hold one of the concrete Chunk types described above.
:cpp:class:`AnyChunk <deephaven::client::chunk::AnyChunk>` is useful in certain limited cases
where a factory method needs to create a
:cpp:class:`Chunk <deephaven::client::chunk::Chunk>`
having a dynamically-determined type, not known at compile time. Of course this could also be
accomplished by returning a heap-allocated pointer to a
:cpp:class:`Chunk <deephaven::client::chunk::Chunk>`.
The rationale for using the variant approach rather than the
heap-allocated object approach is for the sake of simplicity and efficiency when using these
small objects. One example method that returns an
:cpp:class:`AnyChunk <deephaven::client::chunk::AnyChunk>`
is
:cpp:func:`createChunkFor <deephaven::client::chunk::ChunkMaker::createChunkFor>`,
which creates a
:cpp:class:`Chunk <deephaven::client::chunk::Chunk>`
with a type appropriate to the passed-in
:cpp:class:`ColumnSource <deephaven::client::column::ColumnSource>`,
and wraps that dynamicaly-determined Chunk in an
:cpp:class:`AnyChunk <deephaven::client::chunk::AnyChunk>` value.

Chunk Declarations
------------------

.. doxygenclass:: deephaven::client::chunk::Chunk
   :members:

.. doxygenclass:: deephaven::client::chunk::GenericChunk
   :members:

.. doxygentypedef:: deephaven::client::chunk::Int8Chunk

.. doxygentypedef:: deephaven::client::chunk::Int16Chunk

.. doxygentypedef:: deephaven::client::chunk::Int32Chunk

.. doxygentypedef:: deephaven::client::chunk::Int64Chunk

.. doxygentypedef:: deephaven::client::chunk::FloatChunk

.. doxygentypedef:: deephaven::client::chunk::DoubleChunk

.. doxygentypedef:: deephaven::client::chunk::BooleanChunk

.. doxygentypedef:: deephaven::client::chunk::StringChunk

.. doxygentypedef:: deephaven::client::chunk::DateTimeChunk

Utility Declarations
--------------------

.. doxygenclass:: deephaven::client::chunk::AnyChunk
   :members:

.. doxygenclass:: deephaven::client::chunk::ChunkVisitor
   :members:

.. doxygenclass:: deephaven::client::chunk::ChunkMaker
   :members:
