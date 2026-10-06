---
id: common-problems
title: Common problems when debugging Deephaven
---

This guide describes common Deephaven-specific issues you may encounter when debugging.

The examples shown use PyCharm with pip-installed Deephaven, but these problems and solutions apply to all IDEs and installation methods.

> [!NOTE]
> These issues are specific to how Deephaven works and are not related to your debugger setup.

## Lazy evaluation of some table operations

Some Deephaven table operations like [`update_view`](../../reference/table-operations/select/update-view.md) utilize _lazy evaluation_, where the results of the operation are not computed until they are required by another downstream operation. In the absence of such downstream operations, the debugger may not hit the expected parts of the code since no real work is being done. This is particularly salient when attempting to debug user-defined functions called in lazy table operations. Take this example:

![img](../../assets/how-to/debugging/prob-1.png)

In this case, the breakpoint will not be reached, because [`update_view`](../../reference/table-operations/select/update-view.md) does not evaluate `udf`. To force evaluation for debugging purposes, use [`select`](../../reference/table-operations/select/select.md):

![img](../../assets/how-to/debugging/prob-2.png)

## Ticking tables and the main thread

Ticking Deephaven tables are updated on Deephaven's own update threads, not on your script's main thread. Deephaven registers those threads with the debugger, but if your script reaches its last line, the Python process exits and stops the server before any updates run. Again, this is particularly relevant with user-defined functions:

![img](../../assets/how-to/debugging/prob-3.png)

The script ends before the UDF is ever called. To remedy this, keep the process alive with a call to [`time.sleep`](https://docs.python.org/3/library/time.html#time.sleep):

![img](../../assets/how-to/debugging/prob-4.png)

## Related documentation

- [`update_view`](../../reference/table-operations/select/update-view.md)
- [`select`](../../reference/table-operations/select/select.md)
- [Python functions in query strings](../python-functions.md)
- [Table types](../../conceptual/table-types.md)
