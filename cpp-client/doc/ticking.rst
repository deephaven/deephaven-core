Subscribing to Ticking Tables
=============================

Description
-----------

Some C++ applications need to maintain a local copy of a table that stays in sync with the server
side.  This is useful when your code is using custom libraries or other algorithms that are more
natural to execute on the client rather than the server.

To support this functionality,
:cpp:class:`TableHandle <deephaven::client::TableHandle>`
provides the
:cpp:func:`Subscribe <deephaven::client::TableHandle::Subscribe>`
method. When this method is invoked, the Deephaven client library will start maintaining an internal
local copy of the table, keeping it up to date by processing periodic low-level update messages from
the server.  Furthermore, the client library will provide a digested form of these updates to
subscribing code in a user-friendly form. The client library will continue to maintain this
subscription until the user calls
:cpp:func:`Unsubscribe <deephaven::client::TableHandle::Unsubscribe>`.

A simple example is:

.. code:: c++

  TableHandle my_ticking_data = manager.FetchTable("MyTickingData");
  auto sub_handle = my_ticking_data.Subscribe(std::make_shared<MyCallback>());
  // Do all your processing...
  // Then when all done, unsubscribe...
  my_ticking_data.Unsubscribe(std::move(sub_handle));

The user-supplied ``MyCallback`` is a class deriving from
:cpp:class:`TickingCallback <deephaven::dhcore::ticking::TickingCallback>`
which overrides two methods:

* :cpp:func:`OnTick <deephaven::dhcore::ticking::TickingCallback::OnTick>`
* :cpp:func:`OnFailure <deephaven::dhcore::ticking::TickingCallback::OnFailure>`

When table data changes on the server side, those changes are batched up and periodically
transmitted to the client (by default, at a rate of about once per second). First, the client
library processes these low-level messages and applies them to its internal copy of the table. Then,
it constructs a user-friendly
:cpp:class:`TickingUpdate <deephaven::dhcore::ticking::TickingUpdate>`
object and invokes the user's
:cpp:func:`OnTick <deephaven::dhcore::ticking::TickingCallback::OnTick>`
callback, passing it the
:cpp:class:`TickingUpdate <deephaven::dhcore::ticking::TickingUpdate>`
object.

A skeleton of a user-defined callback object looks like this:

.. code:: c++

   class MyCallback final : public deephaven::dhcore::ticking::TickingCallback {
   public:
     void OnTick(deephaven::dhcore::ticking::TickingUpdate update) final {
       // handle the update message
     }

     void OnFailure(std::exception_ptr ep) final {
       // handle the failure
     }
   };

Structure of updates from the server
------------------------------------

Synchronization between the server and client is performed via the
`Barrage <https://deephaven.io/barrage/docs/>`_
protocol. The low-level details of this protocol are outside the scope of this document. However, at
a high level, the purpose of the protocol is to efficiently transmit differences between table versions
as a table changes over time. Those differences include:

* removed rows
* shifts (note 1)
* added rows
* modified cells (note 2)

Notes:

1. shifts are a way to express the renumbering (but not reordering) of internal row keys.  In this
version of the client, we do not expose internal row keys to the caller. So you will not see shifts
represented in the
:cpp:class:`TickingUpdate <deephaven::dhcore::ticking::TickingUpdate>`
class in this version of the client.

2. In the above we explicitly refer to modified *cells* rather than modified *rows*, because
when a row is modified, typically only some cells within that row change but others stay the same.
For the sake of efficiency, the Barrage protocol allows the server to specify the specific
cells that changed within a row. These modifications are represented on a per-column basis. That is,
for each column, the library will indicate (via a
:cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>`)
which rows of that column were modified.

The TickingUpdate class
-----------------------

The
:cpp:class:`TickingUpdate <deephaven::dhcore::ticking::TickingUpdate>`
class represents the changes that have happened to the table since the last tick. It contains
snapshots
(:cpp:func:`Prev <deephaven::dhcore::ticking::TickingUpdate::Prev>`
and
:cpp:func:`Current <deephaven::dhcore::ticking::TickingUpdate::Current>`)
of the table at the start and end of the entire update operation,
as well as intermediate snapshots

* before and after the remove operation,
* before and after the add operation, and
* before and after the modify operation.

It also contains
:cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>`
values representing the positions of the removed, added, and modified items.

For some callers, the per-update
:cpp:func:`Prev <deephaven::dhcore::ticking::TickingUpdate::Prev>`
and
:cpp:func:`Current <deephaven::dhcore::ticking::TickingUpdate::Current>`
table snapshots suffice for their needs.
These snapshots tell the caller how the table looked before
the update and after the update, respectively. Other callers will need more precise
information: exactly what rows were removed, added, and modified. These callers can use the
per-operation snapshots.

The per-update snapshots are:

* :cpp:func:`Prev <deephaven::dhcore::ticking::TickingUpdate::Prev>` - snapshot of the table before any of this cycle's updates were applied.
* :cpp:func:`Current <deephaven::dhcore::ticking::TickingUpdate::Current>` - snapshot of the table after all of this cycle's updates were applied.

The more fine-grained per-operation snaphots are:

* :cpp:func:`BeforeRemoves <deephaven::dhcore::ticking::TickingUpdate::BeforeRemoves>` - snapshot of the table as it appeared before the remove operation
* :cpp:func:`AfterRemoves <deephaven::dhcore::ticking::TickingUpdate::AfterRemoves>` - snapshot of the table as it appeared after the remove operation
* :cpp:func:`BeforeAdds <deephaven::dhcore::ticking::TickingUpdate::BeforeAdds>` - snapshot of the table as it appeared before the add operation
* :cpp:func:`AfterAdds <deephaven::dhcore::ticking::TickingUpdate::AfterAdds>` - snapshot of the table as it appeared after the add operation
* :cpp:func:`BeforeModifies <deephaven::dhcore::ticking::TickingUpdate::BeforeModifies>` - snapshot of the table as it appeared before the modify operation
* :cpp:func:`AfterModifies <deephaven::dhcore::ticking::TickingUpdate::AfterModifies>` - snapshot of the table as it appeared after the modify operation

Some of these snapshots are duplicative: For example, due to the order in which changes are applied
internally, it happens to be the case that
:cpp:func:`AfterRemoves <deephaven::dhcore::ticking::TickingUpdate::AfterRemoves>`
and
:cpp:func:`BeforeAdds <deephaven::dhcore::ticking::TickingUpdate::BeforeAdds>`
refer to exactly the same snapshot. We provide these extra snapshots for the
programmer's convenience and intuition.

The library also takes pains to coalesce snapshots. For example, if no removes happen
in a given update, then the
:cpp:func:`BeforeRemoves <deephaven::dhcore::ticking::TickingUpdate::BeforeRemoves>`
pointer will compare equal to the
:cpp:func:`AfterRemoves <deephaven::dhcore::ticking::TickingUpdate::AfterRemoves>`
pointer.

Some readers may be concerned about the cost of maintaining all these snapshots. Internally,
the snapshots are represented by copy-on-write data structures that take pains to do
a lot of structural sharing. Broadly speaking, it is not expensive to have two snapshots
of a table when most of the data is unchanged between the two tables. The specific
implementation of this snapshotting data structure comes from the
`Immer Persistent and Immutable Data Structures <https://sinusoid.es/immer/>`_
project.

The
:cpp:class:`TickingUpdate <deephaven::dhcore::ticking::TickingUpdate>`
object also provides
:cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>`
objects indicating which specific rows were changed. The provided
:cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>`
objects are:

* :cpp:func:`RemovedRows <deephaven::dhcore::ticking::TickingUpdate::RemovedRows>` - indexes of rows removed from the
  :cpp:func:`BeforeRemoves <deephaven::dhcore::ticking::TickingUpdate::BeforeRemoves>`
  snapshot to form
  :cpp:func:`AfterRemoves <deephaven::dhcore::ticking::TickingUpdate::AfterRemoves>`.
* :cpp:func:`AddedRows <deephaven::dhcore::ticking::TickingUpdate::AddedRows>` - indexes of rows added to the
  :cpp:func:`BeforeAdds <deephaven::dhcore::ticking::TickingUpdate::BeforeAdds>`
  snapshot to form
  :cpp:func:`AfterAdds <deephaven::dhcore::ticking::TickingUpdate::AfterAdds>`.
* :cpp:func:`ModifiedRows <deephaven::dhcore::ticking::TickingUpdate::ModifiedRows>` - a ``std::vector`` of
  :cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>`
  shared_ptrs, which represents the modified data on a per-column basis.
  Each element of the vector is a
  :cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>`
  shared_ptr representing the corresponding column. That
  :cpp:class:`RowSequence <deephaven::dhcore::container::RowSequence>`
  provides the indexes of rows that were modified in the corresponding column of
  :cpp:func:`BeforeModifies <deephaven::dhcore::ticking::TickingUpdate::BeforeModifies>`
  to form the corresponding column in
  :cpp:func:`AfterModifies <deephaven::dhcore::ticking::TickingUpdate::AfterModifies>`.

Declarations
------------

.. doxygenclass:: deephaven::dhcore::clienttable::ClientTable
   :members:

.. doxygenclass:: deephaven::dhcore::clienttable::Schema
   :members:

.. doxygenclass:: deephaven::dhcore::ticking::TickingCallback
   :members:

.. doxygenclass:: deephaven::dhcore::ticking::TickingUpdate
   :members:
