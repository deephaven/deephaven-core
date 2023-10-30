Row Sequences
=============

Description
-----------

A
:cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>`
is an abstract class representing a monotonically-increasing sequence of row numbers that can be
used to reference elements in a
:cpp:class:`ClientTable <deephaven::dhcore::clienttable::ClientTable>` or
:cpp:class:`ColumnSource <deephaven::dhcore::column::ColumnSource>`.

It is used as a parameter to methods like
:cpp:func:`Stream <deephaven::dhcore::clienttable::ClientTable::Stream>` and
:cpp:func:`fillChunk <deephaven::dhcore::column::ColumnSource::FillChunk>`.

The row numbers inside a
:cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>`
are ``uint64_t`` values. The coordinate space used (whether key space or position space)
is not specified, and is implied by context. However, as of this writing, all of the public
methods in the C++ client that take
:cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>` arguments
assume they are in position space.

:cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>`
objects are immutable. They can be sliced via the
:cpp:func:`Take <deephaven::dhcore::container::RowSequence::Take>`
and
:cpp:func:`Drop <deephaven::dhcore::container::RowSequence::Drop>`
methods, which return new
:cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>` shared_ptrs.

You can interrogate their size via
:cpp:func:`size <deephaven::dhcore::container::RowSequence::Size>` or
:cpp:func:`empty <deephaven::dhcore::container::RowSequence::Empty>`.
You can iterate over them with
:cpp:func:`forEachInterval <deephaven::dhcore::container::RowSequence::ForEachInterval>`
or by obtaining a
:cpp:class:`RowSequenceIterator <deephaven::dhcore::container::RowSequenceIterator>`
via
:cpp:func:`getRowSequenceIterator <deephaven::dhcore::container::RowSequence::GetRowSequenceIterator>`

Declarations
------------

.. doxygenclass:: deephaven::dhcore::container::RowSequence
   :members:

.. doxygenclass:: deephaven::dhcore::container::RowSequenceBuilder
   :members:

.. doxygenclass:: deephaven::dhcore::container::RowSequenceIterator
   :members:
