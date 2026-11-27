---
title: Reading data
---

# Reading data

{@link dh.Table}, {@link dh.TotalsTable} and {@link dh.ResolvedTable} read data the same way: a one-off snapshot, a
subscription to a window of rows, or a subscription to the whole table.

## One-off reads

{@link dh.ResolvedTable.createSnapshot | createSnapshot} takes {@link dh.SnapshotOptions} specifying the rows and
columns, and resolves to a {@link dh.TableData} of their current values.

## Viewport subscriptions

{@link dh.ResolvedTable.createViewportSubscription | createViewportSubscription} takes
{@link dh.ViewportSubscriptionOptions} and returns a {@link dh.TableViewportSubscription}, which fires an `"updated"`
event with a {@link dh.ViewportData} for the rows in view when they first arrive and each time they change. The visible
window can be moved with {@link dh.TableViewportSubscription.update | update}.

## Full-table subscriptions

{@link dh.ResolvedTable.createSubscription | createSubscription} takes {@link dh.SubscriptionOptions} and returns a
{@link dh.TableSubscription}, which fires an `"updated"` event with a {@link dh.SubscriptionTableData} listing the rows
added, removed and modified. Neither subscription type has a constant for the event's name yet, so listen for the string
`"updated"`.

## Reading table data

A {@link dh.TableData} gives rows, and cells by row and column, along with any formatting. How rows are indexed depends
on where the data came from. A snapshot or a viewport is indexed from 0 within the rows returned, so a viewport's row
`i` is at table position {@link dh.ViewportData.offset | offset} + `i`. A full subscription is indexed by row key, as
listed in {@link dh.SubscriptionTableData.fullIndex | fullIndex} and the {@link dh.SubscriptionTableData.added | added}
and {@link dh.SubscriptionTableData.modified | modified} sets. Deephaven tables are columnar on the server, over the
wire, and on the client, so reading large tables by column is often more efficient than reading by row.

## Ticking tables

A ticking table's size changes as rows arrive; a {@link dh.Table} fires {@link dh.Table.EVENT_SIZECHANGED} when it does.

## Closing subscriptions

Subscriptions hold server resources: close each one when done, separately from the table it came from.

## Column statistics and seeking

{@link dh.ResolvedTable.getColumnStatistics | getColumnStatistics} gives a {@link dh.ColumnStatistics} for one column,
and {@link dh.ResolvedTable.seekRow | seekRow} finds the next row matching a value. `seekRow` takes a
{@link dh.SeekRowOptions} on a {@link dh.ResolvedTable}, but separate arguments on a
{@link dh.Table.seekRow | dh.Table}; a {@link dh.TotalsTable} has none.
