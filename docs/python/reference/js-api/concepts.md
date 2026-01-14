---
title: Javascript API Concepts
---

Ticking tables are the core feature of Deephaven, and the Deephaven JS API provides the means to create or discover
tables in a format that is familiar to JS developers, generally through asynchronous tools like promises and events.

The configuration of a Deephaven table is immutable, but new tables can easily be created. Sorting, filtering, or
adjusting the columns of a table are all performed by passing the original table back to the server with one or more of
those operations, and a new table will be sent back informing the client that they can now request data based on the
results of the operation.

Table subscriptions consist of the client specifying which rows or columns they want data from, and the server will then
send updates as needed. These updates can take the form of:

- a snapshot, the data for all rows and columns that were requested
- a delta, where only added, modified, and removed rows and columns will be sent. The client can cancel their subscription to a table, and can also request a snapshot be sent without any subscription.

While the server's Table is immutable, the JS API provides Tables that seem to be mutable, so that a given UI component
can be bound to a single Table instance and get updates based on its current configuration. For example, if a new sort
or filter is applied, updates will pause until that change has been applied, then will resume with the new configuration.
If something goes wrong while applying a change, the table will revert to the previously working configuration and resume,
emitting an event to indicate that something went wrong.

## Table subscriptions

Data can be obtained from tables in two basic ways: subscriptions to the entire table, or just to a specific viewport.
Recall that Deephaven table data is column-oriented, so both ways of getting data permit specifying the columns, but
only a viewport subscription allows specifying the desired rows.

In either case, changes in data will arrive in event data that implements the `TableData` interface, which will provide
access to all data currently available. Viewports are generally small enough to be safe to walk all values, but this API
should be used with care on full table subscriptions. Since the data is column-oriented (even on the client), it is
generally more efficient to iterate over all data within a column if possible, rather than reading by row. However, that isn't
always realistic when it comes to rendering data. Given a `TableData` instance, the `rows` property exposes `Row`
instances which can be read using the `Table`'s columns with either `row.get(column)` or `column.get(row)`. If the
desired row is known, `TableData.get(index)` can be used to read a `Row`, or `TableData.getData(index, column)` will read
the value itself.

### Viewports

As the usual mode of operation is to subscribe to a viewport to display in a UI, the API for setting a viewport lets it
be set on the table itself, via `Table.setViewport()`. This method returns a `TableViewportSubscription`, allowing the
viewport to be manipulated outside of the Table, but for most cases this can be ignored. While a viewport is active,
any data update from the server will be propagated as an "update" event on the Table instance, and the current viewport's
data will be made available there and in `Table.getViewportData()`.

When using the viewport in this way, changes made to the table itself will stop the viewport automatically - a new
`Table.setViewport()` call can be made as soon as the change has started, and the viewport will stream data once the
change is finished.

Alternatively, the `TableViewportSubscription` will fire the "updated" event, so after `Table.setViewport` is called, an
event listener can be attached to the result. The viewport instance also exposes a `getViewportData()` method to get the
most recent data available, and a `setViewport` method to change the viewport. If obtained in this way,
`Table.setViewport` can be used to create another viewport while still leaving this one open - any
`TableViewportSubscription` instances need to have `close()` called on them to release their resources. Additionally, if
the table changes, this viewport will not close automatically.

### Full-table subscriptions

For some applications, it doesn't make sense to only subscribe to a limited range of rows, but the entire table is needed.
In a browser-based JS application, charts are probably the most common example of this. Care should be taken to avoid
inundating the client with data, since JS is a singly threaded language, and millions of rows could pause for seconds,
depending on how many columns are being read.

To create a full-table subscription, use `Table.subscribe()` - a `TableSubscription` will be returned, with an "updated"
event when data is available, and 2. a `close()` method to stop the subscription. Unlike viewports, these will not fire
events from the table instance.

<!--
This paragraph will soon refer to viewport subscriptions too and will need to be moved up to the end of "Table
subscriptions". See deephaven-core#188.
-->

Since full-table subscriptions tend to be larger than viewports, there is a specialized API on this data to find only
added, modified, and removed rows. The full set of rows will still be available, but care should be taken to avoid
reading every row with only a few changes for very large tables - instead, use the `added`, `removed`, `modified`
rangesets. Additionally, care should be taken to not reuse values in these or the `fullIndex` rangeset between updates;
internally values may be rearranged.
