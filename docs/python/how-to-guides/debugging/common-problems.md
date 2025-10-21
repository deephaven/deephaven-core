---
id: common-problems
title: Common problems when debugging Deephaven
sidebar_label: Common problems
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

Currently, Deephaven only notifies Python debuggers of its main thread. However, ticking Deephaven tables spawn new Java threads where evaluation actually happens. If the main thread is not explicitly kept alive, it will shut down before the debugger can reach operations that happen on the other thread. Again, this is particularly relevant with user-defined functions:

![img](../../assets/how-to/debugging/prob-3.png)

The main thread here shuts down before the UDF is reached. To remedy this, explicitly keep the main thread alive with a call to [`time.sleep`](https://docs.python.org/3/library/time.html#time.sleep):

![img](../../assets/how-to/debugging/prob-4.png)
