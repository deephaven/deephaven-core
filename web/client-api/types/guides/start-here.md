---
title: Start here
---

# Start here

The JS API connects to a Deephaven server from a browser or Node, and reads its tables, including tables that tick. It
offers two ways to work with a table: a mutable {@link dh.Table}, whose filters, sorts and custom columns change in
place, which suits UI state such as a grid; and {@link dh.TableOperations}, where each operation makes a new server-side
table, which suits building queries. {@link dh.Table.operations | operations} and
{@link dh.ResolvedTable.createMutableTable | createMutableTable} convert between the two.
