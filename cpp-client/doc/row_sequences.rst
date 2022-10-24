Row Sequences
=============

Description
-----------

A
:cpp:class:`RowSequence <deephaven::client::container::RowSequence>`
is an abstract class representing a monotonically-increasing sequence of row numbers that can be
used to reference elements in a
:cpp:class:`Table <deephaven::client::table::Table>` or
:cpp:class:`ColumnSource <deephaven::client::column::ColumnSource>`.

It is used as a parameter to methods like
:cpp:func:`stream <deephaven::client::table::Table::stream>` and
:cpp:func:`fillChunk <deephaven::client::column::ColumnSource::fillChunk>`.

The row numbers inside a
:cpp:class:`RowSequence <deephaven::client::container::RowSequence>`
are ``uint64_t`` values. The coordinate space used (whether key space or position space)
is not specified, and is implied by context. However, as of this writing, all of the public
methods in the C++ client that take
:cpp:class:`RowSequence <deephaven::client::container::RowSequence>` arguments
assume they are in position space.

:cpp:class:`RowSequence <deephaven::client::container::RowSequence>`
objects are immutable. They can be sliced via the
:cpp:func:`take <deephaven::client::container::RowSequence::take>`
and
:cpp:func:`drop <deephaven::client::container::RowSequence::drop>`
methods, which return new
:cpp:class:`RowSequence <deephaven::client::container::RowSequence>` shared_ptrs.

You can interrogate their size via
:cpp:func:`size <deephaven::client::container::RowSequence::size>` or
:cpp:func:`empty <deephaven::client::container::RowSequence::empty>`.
You can iterate over them with
:cpp:func:`forEachInterval <deephaven::client::container::RowSequence::forEachInterval>`
or by obtaining a
:cpp:class:`RowSequenceIterator <deephaven::client::container::RowSequenceIterator>`
via
:cpp:func:`getRowSequenceIterator <deephaven::client::container::RowSequence::getRowSequenceIterator>`

Declarations
------------

.. doxygenclass:: deephaven::client::container::RowSequence
   :members:

.. doxygenclass:: deephaven::client::container::RowSequenceBuilder
   :members:

.. doxygenclass:: deephaven::client::container::RowSequenceIterator
   :members:
