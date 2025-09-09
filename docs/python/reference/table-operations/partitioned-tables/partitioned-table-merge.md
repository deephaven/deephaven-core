---
title: merge
---

The `merge` method vertically stacks all constituent tables of a `PartitionedTable` to produce a new table in the same way that [`merge`](../merge/merge.md) vertically stacks a list of tables.

## Syntax

```python syntax
merge() -> Table
```

## Parameters

This method takes no parameters.

## Returns

A new table containing the contents of all of the constituent tables of a `PartitionedTable`.

## Example

In the following example, the `source` table is partitioned by `IntCol`, which has 3 unique values. Thus, `partitioned_table` has 3 constituent tables. Merging `partitioned_table` into `result` shows that `result` contains all of the same contents as `source`, but in a different order. Partitioned tables get merged based on the order of the constituent tables.

```python order=result,source
from deephaven import empty_table

source = empty_table(10).update(["IntCol = i%3", "StrCol = `value`"])
partitioned_table = source.partition_by(["IntCol"])

result = partitioned_table.merge()
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable.merge)
