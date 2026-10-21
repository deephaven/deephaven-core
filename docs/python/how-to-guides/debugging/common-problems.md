---
id: common-problems
title: Common problems when debugging Deephaven
---

This guide covers two Deephaven-specific reasons the debugger doesn't stop at a breakpoint in your code: table operations that evaluate lazily, and ticking-table updates that never run because a script that starts its own server exits first. Neither is caused by your debugger setup.

The examples use PyCharm with [pip-installed Deephaven](./pip-setup.md), but these problems and solutions apply to all IDEs and, except where noted, all installation methods. Use PyCharm 2024 or later. Debugging Deephaven with PyCharm 2023 has known problems.

## Lazy evaluation of some table operations

Some Deephaven table operations, including [`view`](../../reference/table-operations/select/view.md), [`update_view`](../../reference/table-operations/select/update-view.md), and [`lazy_update`](../../reference/table-operations/select/lazy-update.md), use _lazy evaluation_. Deephaven doesn't compute a column's values until something reads them, for example a downstream operation or opening the table in the Deephaven web IDE. Until then, code in the [formula](../formulas.md) never runs, so the debugger doesn't hit breakpoints in it.

This matters most when debugging [user-defined functions](../python-functions.md) called in lazy table operations. Take this example, which sets a breakpoint on the first line of `udf`:

```python skip-test
from deephaven_server import Server

s = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
        "-Dprocess.info.system-info.enabled=false",
    ],
)
s.start()

from deephaven import empty_table


def udf(x) -> int:
    y = x + 1  # breakpoint here
    return y


t = empty_table(10).update("X = ii")

# update_view is lazily evaluated, so udf is not called
t_new = t.update_view("Y = udf(X)")
```

> [!NOTE]
> The `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler` argument turns on [anonymous authentication](../authentication/auth-anon.md), so you can open the Deephaven web IDE without a [pre-shared key](../authentication/auth-psk.md). The debugger doesn't need it. With it on, any client that can reach port 10000 can run code without a password, so use it only on a machine and network you trust.

In this case, the debugger does not reach the breakpoint, because [`update_view`](../../reference/table-operations/select/update-view.md) does not evaluate `udf`. The script prints `Server started on port 10000` and then finishes without pausing at the breakpoint.

To force evaluation for debugging purposes, use [`select`](../../reference/table-operations/select/select.md). Replace the last two lines of the script with:

```python skip-test
# force evaluation of udf so the breakpoint is reached
t_new = t.update_view("Y = udf(X)").select()
```

The debugger now stops at the breakpoint inside `udf`:

![PyCharm debugger stopped at the breakpoint in udf after adding select](../../assets/how-to/debugging/prob-2.png)

## Ticking tables when your script starts the server

Your script may start the server itself with `deephaven_server.Server`, as in the [pip-installed setup](./pip-setup.md) or the [embedded-server setup](./embedded-setup.md). That server runs only as long as the Python process does, so it stops when the script reaches its last line.

Deephaven applies [ticking table](../../conceptual/table-types.md) updates on its own update threads, separately from your script's main thread. If the process exits before the next update, the code that update would call never runs.

This doesn't affect code run in the web console of a separately running server, such as one [run in Docker](./docker-setup.md), because that server keeps running after the code finishes.

This problem most often shows up when you debug a [user-defined function (UDF)](../python-functions.md) called from a [ticking table](../../conceptual/table-types.md) operation:

```python skip-test
from deephaven_server import Server

s = Server(
    port=10000,
    jvm_args=[
        "-Xmx4g",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
        "-Dprocess.info.system-info.enabled=false",
    ],
)
s.start()

from deephaven import time_table


def udf(x) -> int:
    y = x + 1  # breakpoint here
    return y


t = time_table("PT1s").update("X = ii")

# the script ends, and the server stops, before an update calls udf
t_new = t.update("Y = udf(X)")
```

The script ends before the UDF is ever called, and the debugger never stops at the breakpoint. To remedy this in a script that starts its own server, keep the process alive with a call to [`time.sleep`](https://docs.python.org/3/library/time.html#time.sleep) at the end of the script:

```python skip-test
# keep the process alive so the server keeps running and updates call udf
from time import sleep

sleep(30)
```

The debugger now stops at the breakpoint in `udf`, on one of Deephaven's update threads. Deephaven registers its update threads with the debugger, so the thread list shows a Deephaven thread rather than `MainThread`. In this screenshot, it is `Dummy-8`:

![PyCharm debugger stopped at the breakpoint in udf on an update thread after adding sleep](../../assets/how-to/debugging/prob-4.png)

## Related documentation

- [Table types](../../conceptual/table-types.md)
- [Formulas](../formulas.md)
- [Python functions in query strings](../python-functions.md)
- [Debug pip-installed Deephaven](./pip-setup.md)
- [Debug an embedded Deephaven server with PyCharm](./embedded-setup.md)
- [Debug Docker-run Deephaven with PyCharm](./docker-setup.md)
- [Enable anonymous authentication](../authentication/auth-anon.md)
- [`empty_table`](../../reference/table-operations/create/emptyTable.md)
- [`lazy_update`](../../reference/table-operations/select/lazy-update.md)
- [`select`](../../reference/table-operations/select/select.md)
- [`time_table`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`update_view`](../../reference/table-operations/select/update-view.md)
- [`view`](../../reference/table-operations/select/view.md)
