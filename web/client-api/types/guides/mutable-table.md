---
title: Working with a dh.Table
---

# Working with a dh.Table

A {@link dh.Table} is built for user interfaces. It is one object standing for a view of a server-side table whose sort,
filter, and custom columns change as the user works, such as clicking a grid's column header or typing in a filter box,
and it fires events so the interface can redraw. Changes apply on the server asynchronously: the table's properties
return the new values at once, and an event fires once the server has applied them.

To build queries in code instead, where each step makes a new table in arbitrary order and with more operations and
control, see [Table operations by example](table-operations.md).

## Getting a table

{@link dh.IdeSession.getTable | getTable} gives a `PendingTable<Table>`. Awaiting it gives a {@link dh.Table}.
{@link dh.IdeSession.getObject | getObject} gives the same for a table, but is typed
`PendingTable<Table> | Promise<any>` - cast it to `dh.PendingTable<dh.Table>` or narrow it before use. 

A bare `dh.PendingTable` means `PendingTable<ResolvedTable>`, what table operations produce. Given a 
{@link dh.ResolvedTable}, invoke {@link dh.ResolvedTable.createMutableTable | createMutableTable} to get a 
{@link dh.Table}. Changing the new table does not affect the {@link dh.ResolvedTable}, the two are closed separately.

## Changing the sort, filter and columns

{@link dh.Table.applySort | applySort} takes {@link dh.Sort}s from {@link dh.Column.sort | Column.sort}, with
{@link dh.Sort.asc | asc}, {@link dh.Sort.desc | desc} and {@link dh.Sort.abs | abs}.
{@link dh.Table.applyFilter | applyFilter} takes the conditions described in [Filters](filters.md).
{@link dh.Table.applyCustomColumns | applyCustomColumns} adds columns computed on the server, written as
`"Name = expression"`.

Each call replaces everything set before it and returns the previous value. The calls need not wait for each other:
make all the changes, then wait for the event that matters, or read the data. {@link dh.Table.sort | sort},
{@link dh.Table.filter | filter} and {@link dh.Table.customColumns | customColumns} return the new values right away,
while rows can still arrive under the old ones until the matching event fires.

{@includeCode ../examples/use-cases.ts#changeMutableTable}

## Events

{@link dh.Table.addEventListener | addEventListener} registers a listener and returns a function that removes it.
{@link dh.Table.nextEvent | nextEvent} gives a promise of the next event of one type.

| Event | Fires when |
|---|---|
| {@link dh.Table.EVENT_SORTCHANGED} | a new sort has applied |
| {@link dh.Table.EVENT_FILTERCHANGED} | new filters have applied |
| {@link dh.Table.EVENT_CUSTOMCOLUMNSCHANGED} | new custom columns have applied, so column headers can update |
| {@link dh.Table.EVENT_REQUEST_SUCCEEDED} | a pending change applied on the server, before the event for that change |
| {@link dh.Table.EVENT_REQUEST_FAILED} | the server rejected a change or failed; the table returns to its last working state |
| {@link dh.Table.EVENT_SIZECHANGED} | the number of rows changed, such as on a ticking table |
| {@link dh.Table.EVENT_DISCONNECT} | the connection was lost; nothing updates until it returns |
| {@link dh.Table.EVENT_RECONNECT} | the table is back, and its viewports and subscriptions were requested again |
| {@link dh.Table.EVENT_RECONNECTFAILED} | the table could not be brought back, and a new one must be fetched |

{@includeCode ../examples/use-cases.ts#listenToMutableTable}

## Reading data

A {@link dh.Table} reads data the same way the other table types do, through snapshots and subscriptions; see
[Reading data](read-data.md), and [Values](values.md) for what each column type gives. Its
{@link dh.Table.EVENT_UPDATED} and row events belong to the older viewport set on the table itself with
{@link dh.Table.setViewport | setViewport}, which a subscription replaces.

## Copies and table operations

{@link dh.Table.copy | copy} gives a new {@link dh.Table} as this one is now, to sort and filter separately; it must be
closed too.
{@link dh.Table.operations | operations} starts [table operations](table-operations.md) from the table as it is at that
moment, and later changes to the table do not affect them; {@link dh.ResolvedTable.createMutableTable |
createMutableTable} goes the other way.

## Closing

{@link dh.Table.close | close} tells the server the table is no longer used, so it can release the table's resources.
