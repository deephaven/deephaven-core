---
title: update_graph
---

The `update_graph` method returns the source table's update graph.

## Syntax

```
source.update_graph
```

## Parameters

`update_graph` does not take any arguments.

## Returns

The source table's update graph.

## Example

```python order=:log
from deephaven import empty_table

source = empty_table(5).update(["IntCol = i", "StrCol = (IntCol > 2) ? `A` : `B`"])

print(source.update_graph)
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update_graph)
