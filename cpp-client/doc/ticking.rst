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
:cpp:func:`subscribe <deephaven::client::TableHandle::subscribe>`
method. When this method is invoked, the Deephaven client library will start maintaining an internal
local copy of the table, keeping it up to date by processing periodic low-level update messages from
the server.  Furthermore, the client library will provide a digested form of these updates to
subscribing code in a user-friendly form. The client library will continue to maintain this
subscription until the user calls
:cpp:func:`unsubscribe <deephaven::client::TableHandle::unsubscribe>`.

A simple example is:

.. code:: c++

   TableHandle myTickingData = manager.fetchTable("MyTickingData");
   auto subscriptionHandle = myTickingData.subscribe(std::make_shared<MyCallback>());
   // Do all your processing...
   // Then when all done, unsubscribe...
   myTickingData.unsubscribe(std::move(subscriptionHandle));

The user-supplied ``MyCallback`` is a class deriving from
:cpp:class:`TickingCallback <deephaven::client::TickingCallback>`
which overrides two methods:

* :cpp:func:`onTick <deephaven::client::TickingCallback::onTick>`
* :cpp:func:`onFailure <deephaven::client::TickingCallback::onFailure>`

When table data changes on the server side, those changes are batched up and periodically
transmitted to the client (by default, at a rate of about once per second). First, the client
library processes these low-level messages and applies them to its internal copy of the table. Then,
it constructs a user-friendly
:cpp:class:`TickingUpdate <deephaven::client::TickingUpdate>`
object and invokes the user's
:cpp:func:`onTick <deephaven::client::TickingCallback::onTick>`
callback, passing it the
:cpp:class:`TickingUpdate <deephaven::client::TickingUpdate>`
object.

A skeleton of a user-defined callback object looks like this:

.. code:: c++

   class MyCallback final : public deephaven::client::TickingCallback {
   public:
     void onTick(deephaven::client::TickingUpdate update) final {
       // handle the update message
     }

     void onFailure(std::exception_ptr ep) final {
       // handle the failure
     }
   };

Structure of updates from the server
------------------------------------

Synchronization between the server and client is performed via the
`Barrage <https://deephaven.io/barrage/docs/>`_
protocol. The low-level details of this protocol are outside the scope of this document. However, at
a high level, the purpose of the protocol is to efficiently transmit "diffs" between table versions
as a table changes over time. Those diffs include:

* removed rows
* shifts (note 1)
* added rows
* modified cells (note 2)

Notes:

1. shifts are a way to express the renumbering (but not reordering) of internal row keys.  In this
version of the client, we do not expose internal row keys to the caller. So you will not see shifts
represented in the
:cpp:class:`TickingUpdate <deephaven::client::TickingUpdate>`
class in this version of the client.

2. In the above we explicitly refer to modified *cells* rather than modified *rows*, because
when a row is modified, typically only some cells within that row change but others stay the same.
For the sake of efficiency, the Barrage protocol allows the server to specify the specific
cells that changed within a row. These modifications are represented on a per-column basis. That is,
for each column, the library will indicate (via a
:cpp:class:`RowSequence <deephaven::client::container::RowSequence>`)
which rows of that column were modified.

The TickingUpdate class
-----------------------

The
:cpp:class:`TickingUpdate <deephaven::client::TickingUpdate>`
class represents the changes that have happened to the table since the last tick. It contains
snapshots
(:cpp:func:`prev <deephaven::client::TickingUpdate::prev>`
and
:cpp:func:`current <deephaven::client::TickingUpdate::current>`)
of the table at the start and end of the entire update operation,
as well as intermediate snapshots

* before and after the remove operation,
* before and after the add operation, and
* before and after the modify operation.

It also contains
:cpp:class:`RowSequence <deephaven::client::container::RowSequence>`
values representing the positions of the removed, added, and modified items.

For some callers, the per-update
:cpp:func:`prev <deephaven::client::TickingUpdate::prev>`
and
:cpp:func:`current <deephaven::client::TickingUpdate::current>`
table snapshots suffice for their needs.
These snapshots tell the caller how the table looked before
the update and after the update, respectively. Other callers will need more precise
information: exactly what rows were removed, added, and modified. These callers can use the
per-operation snapshots.

The per-update snapshots are:

* :cpp:func:`prev <deephaven::client::TickingUpdate::prev>` - snapshot of the table before any of this cycle's updates were applied.
* :cpp:func:`current <deephaven::client::TickingUpdate::current>` - snapshot of the table after all of this cycle's updates were applied.

The more fine-grained per-operation snaphots are:

* :cpp:func:`beforeRemoves <deephaven::client::TickingUpdate::beforeRemoves>` - snapshot of the table as it appeared before the remove operation
* :cpp:func:`afterRemoves <deephaven::client::TickingUpdate::afterRemoves>` - snapshot of the table as it appeared after the remove operation
* :cpp:func:`beforeAdds <deephaven::client::TickingUpdate::beforeAdds>` - snapshot of the table as it appeared before the add operation
* :cpp:func:`afterAdds <deephaven::client::TickingUpdate::afterAdds>` - snapshot of the table as it appeared after the add operation
* :cpp:func:`beforeModifies <deephaven::client::TickingUpdate::beforeModifies>` - snapshot of the table as it appeared before the modify operation
* :cpp:func:`afterModifies <deephaven::client::TickingUpdate::afterModifies>` - snapshot of the table as it appeared after the modify operation

Some of these snapshots are duplicative: For example, due to the order in which changes are applied
internally, it happens to be the case that
:cpp:func:`afterRemoves <deephaven::client::TickingUpdate::afterRemoves>`
and
:cpp:func:`beforeAdds <deephaven::client::TickingUpdate::beforeAdds>`
refer to exactly the same snapshot. We provide these extra snapshots for the
programmer's convenience and intuition.

The library also takes pains to coalesce snapshots. For example, if no removes happen
in a given update, then the
:cpp:func:`beforeRemoves <deephaven::client::TickingUpdate::beforeRemoves>`
pointer will compare equal to the
:cpp:func:`afterRemoves <deephaven::client::TickingUpdate::afterRemoves>`
pointer.

Some readers may be concerned about the cost of maintaining all these snapshots. Internally,
the snapshots are represented by copy-on-write data structures that take pains to do
a lot of structural sharing. Broadly speaking, it is not expensive to have two snapshots
of a table when most of the data is unchanged between the two tables. The specific
implementation of this snapshotting data structure comes from the
`Immer Persistent and Immutable Data Structures <https://sinusoid.es/immer/>`_
project.

The
:cpp:class:`TickingUpdate <deephaven::client::TickingUpdate>`
object also provides
:cpp:class:`RowSequence <deephaven::client::container::RowSequence>`
objects indicating which specific rows were changed. The provided
:cpp:class:`RowSequence <deephaven::client::container::RowSequence>`
objects are:

* :cpp:func:`removedRows <deephaven::client::TickingUpdate::removedRows>` - indexes of rows removed from the
  :cpp:func:`beforeRemoves <deephaven::client::TickingUpdate::beforeRemoves>`
  snapshot to form
  :cpp:func:`afterRemoves <deephaven::client::TickingUpdate::afterRemoves>`.
* :cpp:func:`addedRows <deephaven::client::TickingUpdate::addedRows>` - indexes of rows added to the
  :cpp:func:`beforeAdds <deephaven::client::TickingUpdate::beforeAdds>`
  snapshot to form
  :cpp:func:`afterAdds <deephaven::client::TickingUpdate::afterAdds>`.
* :cpp:func:`modifiedRows <deephaven::client::TickingUpdate::modifiedRows>` - a ``std::vector`` of
  :cpp:class:`RowSequence <deephaven::client::container::RowSequence>`
  shared_ptrs, which represents the modified data on a per-column basis.
  Each element of the vector is a
  :cpp:class:`RowSequence <deephaven::client::container::RowSequence>`
  shared_ptr representing the corresponding column. That
  :cpp:class:`RowSequence <deephaven::client::container::RowSequence>`
  provides the indexes of rows that were modified in the corresponding column of
  :cpp:func:`beforeModifies <deephaven::client::TickingUpdate::beforeModifies>`
  to form the corresponding column in
  :cpp:func:`afterModifies <deephaven::client::TickingUpdate::afterModifies>`.

Declarations
------------

.. doxygenclass:: deephaven::client::table::Table
   :members:

.. doxygenclass:: deephaven::client::table::Schema
   :members:

.. doxygenclass:: deephaven::client::TickingCallback
   :members:

.. doxygenclass:: deephaven::client::TickingUpdate
   :members:
