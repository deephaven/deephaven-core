---
title: How do I shift values in columns?
sidebar_label: How do I shift values in columns?
---

Use special variables [`i` or `ii`](../query-language/variables/special-variables.md) if your table is static and append-only. If your table is ticking, or not append-only, use [`update_by`](../table-operations/update-by-operations/updateBy.md) with [`rolling_group_tick`](../table-operations/update-by-operations/rolling-group-tick.md). Using `i` looks like:

```python skip-test
result = source.update(["ColShiftedUpOne = Col_[i - 1]", "ColShiftedDownOne = Col_[i + 1]]")
```
