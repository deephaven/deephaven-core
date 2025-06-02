Containers
==========

Description
-----------

The Container type is used when a column source needs to store a list
rather than a scalar element. Such a situation arises, for example, when
you have a Deephaven `group_by` operation.

In this situation you will have a
:cpp:type:`ContainerBaseColumnSource <deephaven::dhcore::column::ContainerBaseColumnSource>`
representing the column. To get the data out, you will use
:cpp:func:`FillChunk <deephaven::dhcore::column::ColumnSource::FillChunk>`
to fill a
:cpp:type:`ContainerBaseChunk <deephaven::dhcore::chunk::ContainerBaseChunk>`.
Now you will have a Chunk containing the lists you want to look at.
Each of the elements in this Chunk is a
std::shared_ptr<:cpp:type:`ContainerBase <deephaven::dhcore::container::ContainerBase>`>

However, to get at the elements inside these lists, you need to downcast the
std::shared_ptr<:cpp:type:`ContainerBase <deephaven::dhcore::container::ContainerBase>`>
to the correct derived type, e.g.
std::shared_ptr< :cpp:type:`Container <deephaven::dhcore::container::Container>` <int32_t>>.

If you already know the element type you need, you can use a method like
:cpp:func:`AsContainerPtr <deephaven::dhcore::container::ContainerBase::AsContainerPtr>`
to downcast it directly. If not, you can use the
:cpp:type:`ContainerVisitor <deephaven::dhcore::container::ContainerVisitor>`
to determine the type at runtime. Another way of determining the type at runtime is to use the
:cpp:type:`ColumnSource <deephaven::dhcore::column::ColumnSource>`'s
:cpp:func:`GetElementType <deephaven::dhcore::column::ColumnSource::GetElementType>`
method. The
:cpp:class:`ElementType <deephaven::dhcore::ElementType>`
you get will represent a list type, and you can use its
:cpp:func:`Id <deephaven::dhcore::ElementType::Id>` method to retrieve its
:cpp:enum:`ElementTypeId::Enum <deephaven::dhcore::ElementTypeId::Enum>`.
     

Declarations
------------

.. doxygenclass:: deephaven::dhcore::container::ContainerBase
   :members:

.. doxygenclass:: deephaven::dhcore::container::Container
   :members:

Utility Declarations
--------------------

.. doxygenclass:: deephaven::dhcore::container::ContainerVisitor
   :members:
