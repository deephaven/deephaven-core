---
title: Set table density
---

You can change how densely the Deephaven web UI displays tables to see more or less data in a single view. The **Default table density** setting applies to every table in the web UI, including tables that are already open. It has three options: **Regular** (the default), **Compact**, and **Spacious**.

To change the density of one table instead, [set it in code](#set-the-density-of-one-table) with [`deephaven.ui`](/core/ui/docs/).

The image below shows the three table densities side by side. The **Compact** density uses shorter rows and column headers, smaller cell text, and less header padding, so it shows more rows and columns in the same space. The **Spacious** density uses taller rows and more padding, so it shows fewer rows and columns in the same space.

![Three Deephaven tables side by side showing compact, regular, and spacious densities](../../assets/how-to/ui/table-density.png)

## Set the default table density

To change the default:

1. Open the **Settings** menu. See [Navigate the GUI](./navigating-the-ui.md#top-bar) for where to find it.
2. Expand the **Theme** section.
3. Choose **Regular**, **Compact**, or **Spacious** from the **Default table density** dropdown.

![Settings menu showing the default table density options](../../assets/how-to/ui/set-table-density.png)

## Set the density of one table

To set the density of one table, wrap it in [`ui.table`](/core/ui/docs/components/table/) from [`deephaven.ui`](/core/ui/docs/) and pass `density="compact"`, `density="regular"`, or `density="spacious"`. The argument overrides the **Default table density** setting for that table. A [`ui.table`](/core/ui/docs/components/table/) without a `density` argument uses the setting.

The following example wraps a 100-row table in [`ui.table`](/core/ui/docs/components/table/) with compact density:

```python order=null
from deephaven import empty_table
from deephaven import ui

source = empty_table(100).update(["X = i", "Y = X * X"])

compact_table = ui.table(source, density="compact")
```

The `compact_table` panel shows `source` with compact rows, whatever the **Default table density** setting is.

## Related documentation

- [Navigate the GUI](./navigating-the-ui.md)
- [Create real-time dashboards with `deephaven.ui`](../deephaven-ui.md)
- [`deephaven.ui` crash course](../../getting-started/crash-course/deephaven-ui.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`ui.table`](/core/ui/docs/components/table/)
