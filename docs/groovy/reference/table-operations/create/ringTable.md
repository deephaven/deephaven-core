---
title: RingTableTools.of
---

The `RingTableTools.of` method creates a ring table that retains the latest `capacity` number of rows from the parent table.
Latest rows are determined by the new rows added to the parent table. Deleted rows are ignored, and updated rows are not expected and will raise an exception.

## Syntax

```groovy syntax
RingTableTools.of(parent, capacity)
RingTableTools.of(parent, capacity, initialize)
```

## Parameters

<ParamTable>
<Param name="parent" type="Table">

The parent table.

</Param>
<Param name="capacity" type="int">

The capacity of the ring table.

</Param>
<Param name="initialize" type="boolean">

Whether to initialize the ring table with a snapshot of the parent table. Default is `True`.

</Param>
</ParamTable>

## Returns

An in-memory ring table.

## Examples

The following example creates a table with one column of three integer values.

```groovy order=source,result
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

source = newTable(
  intCol("IntegerColumn", 1, 2, 3, 4, 5)
)

result = RingTableTools.of(source, 3)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`emptyTable`](./emptyTable.md)
- [`intCol`](./intCol.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/table/impl/sources/ring/RingTableTools.html)
