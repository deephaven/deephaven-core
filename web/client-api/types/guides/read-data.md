---
title: Reading data
---

# Reading data

{@link dh.Table}, {@link dh.TotalsTable} and {@link dh.ResolvedTable} read data the same way: a one-off snapshot, a
subscription to a window of rows, or a subscription to the whole table.

## One-off reads

{@link dh.ResolvedTable.createSnapshot | createSnapshot} takes {@link dh.SnapshotOptions} naming the rows and columns,
and gives a {@link dh.TableData} of their current values.

## Viewport subscriptions

{@link dh.ResolvedTable.createViewportSubscription | createViewportSubscription} takes
{@link dh.ViewportSubscriptionOptions} and returns a {@link dh.TableViewportSubscription}, whose updates carry a
{@link dh.ViewportData} for the rows in view; change the window with
{@link dh.TableViewportSubscription.update | update}.

## Full-table subscriptions

{@link dh.ResolvedTable.createSubscription | createSubscription} takes {@link dh.SubscriptionOptions} and returns a
{@link dh.TableSubscription}, whose updates carry a {@link dh.SubscriptionTableData} with the rows added, removed and
modified.

## Reading table data

A {@link dh.TableData} gives rows and cells by position and by column, along with any formatting.

## Ticking tables

A ticking table's size changes as rows arrive; a {@link dh.Table} fires {@link dh.Table.EVENT_SIZECHANGED} when it
does.

## Closing subscriptions

Subscriptions hold server resources: close each one when done, separately from the table it came from.

## Column statistics and seeking

{@link dh.ResolvedTable.getColumnStatistics | getColumnStatistics} gives a {@link dh.ColumnStatistics} for one column,
and {@link dh.ResolvedTable.seekRow | seekRow} finds the next row matching a value.
