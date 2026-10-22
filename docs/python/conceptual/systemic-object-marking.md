---
title: Systemic object marking
---

In Deephaven, a **systemic object** is an object whose failure the engine reports to the server, in addition to logging it. The engine logs every failure, but it reports only failures of systemic objects.

What happens after a systemic failure depends on the server:

- Deephaven Community Core writes an extra `Error in table update` entry to the server log and keeps running.
- Deephaven Core+ (Enterprise) terminates the worker.

Reporting is separate from failure propagation. When a table fails, the tables that depend on it also fail, whether or not they are systemic.

Systemic object marking lets you decide which objects are systemic:

- When marking is disabled (the default), the engine treats every table and [single-table listener](../how-to-guides/table-listeners-python.md) as systemic. Listeners registered with [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen) are the [exception](#manage-thread-systemic-status).
- When marking is enabled, a new table, or a [table listener](../how-to-guides/table-listeners-python.md) that listens to a single table, takes its status from the thread that creates it. It is systemic if that thread is marked systemic, and non-systemic otherwise.
- Deephaven marks its own [update graph](./dag.md) threads systemic. Other threads, including the one that runs your console code, are non-systemic unless you mark them.

When the engine creates tables later for an operation you ran, such as new constituents of a [partitioned table](../how-to-guides/partitioned-tables.md), they take the status of the thread that ran the operation, not that of the [update graph](./dag.md) thread.

Systemic object marking is most useful on Core+, where it lets a worker keep running when a non-critical table fails, such as one created while exploring data in the console. On Community Core, it decides only whether a failure also produces that `Error in table update` log entry.

Systemic objects are unrelated to the systemic [execution context](./execution-context.md) that a script session owns.

This guide first shows what happens when a table fails. It then explains how to enable marking, how to control which objects are systemic with the [`deephaven.systemic_obj_tracker`](/core/pydoc/code/deephaven.systemic_obj_tracker.html) module or by marking an existing table, and how to set the systemic status of a script you run from the [Python client](../getting-started/pyclient-quickstart.md).

## What happens when a table fails

The difference between systemic and non-systemic objects is most visible when a [ticking table](./table-types.md) fails. In this example, the [formula](../how-to-guides/formulas.md) `Z = Y.toString()` throws a `NullPointerException` once `X` exceeds 10 and `Y` becomes null:

```python should-fail
from deephaven import time_table

time_bomb = time_table("PT1s").update(
    ["X = ii", "Y = X > 10 ? null : `abc`", "Z = Y.toString()"]
)
```

With marking enabled, console code runs on a non-systemic thread, so `time_bomb` is non-systemic unless you [mark it systemic](#set-systemic-status-for-a-block-of-code). With marking disabled, `time_bomb` is systemic.

After about 11 seconds, the table fails. Whether or not `time_bomb` is systemic, it and the tables that depend on it fail, and the engine logs the error. The difference is whether the engine also reports the error:

- If `time_bomb` is systemic, the engine reports the error. A Community Core server writes the `Error in table update` log entry and keeps running, and a Core+ worker terminates.
- If `time_bomb` is non-systemic, the engine doesn't report its failure. A Community Core server writes no `Error in table update` log entry, and a Core+ worker keeps running.

A non-systemic table's failure can still be reported through an object that depends on it. If a systemic table or [listener](../how-to-guides/table-listeners-python.md) depends on `time_bomb`, that object also fails, the engine reports its failure, and a Core+ worker terminates.

## Enable systemic object marking

Systemic object marking is disabled by default, and you can't turn it on or off while the server is running. To enable it, set the `SystemicObjectTracker.enabled` property to `true` when you start the server, for example with the JVM option:

```shell
-DSystemicObjectTracker.enabled=true
```

See [how to set configuration properties](../how-to-guides/configuration/configuration-properties.md#how-to-set-properties) for the ways to pass a JVM option or set a property in a [configuration file](../how-to-guides/configuration/config-file.md).

Check whether systemic object marking is enabled with [`is_systemic_object_marking_enabled`](/core/pydoc/code/deephaven.systemic_obj_tracker.html#deephaven.systemic_obj_tracker.is_systemic_object_marking_enabled):

```python test-set=1 order=:log
from deephaven.systemic_obj_tracker import is_systemic_object_marking_enabled

print(is_systemic_object_marking_enabled())
```

## Manage thread systemic status

Because systemic status comes from the creating thread, you control which tables and [table listeners](../how-to-guides/table-listeners-python.md) are systemic by setting the status of the thread that creates them. The exception is a listener registered with [`merged_listen`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen). It is never systemic, whether or not marking is enabled, so the engine never reports its failures.

Use [`is_systemic`](/core/pydoc/code/deephaven.systemic_obj_tracker.html#deephaven.systemic_obj_tracker.is_systemic) to check the current thread's status and [`set_systemic`](/core/pydoc/code/deephaven.systemic_obj_tracker.html#deephaven.systemic_obj_tracker.set_systemic) to change it. If systemic object marking is not enabled, [`is_systemic`](/core/pydoc/code/deephaven.systemic_obj_tracker.html#deephaven.systemic_obj_tracker.is_systemic) always returns `False`, even though every table is then systemic, and [`set_systemic`](/core/pydoc/code/deephaven.systemic_obj_tracker.html#deephaven.systemic_obj_tracker.set_systemic) raises a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError).

This example checks [`is_systemic_object_marking_enabled`](/core/pydoc/code/deephaven.systemic_obj_tracker.html#deephaven.systemic_obj_tracker.is_systemic_object_marking_enabled) first. When marking is enabled, it marks the current thread systemic and prints the thread's new status. Otherwise, it prints a message:

```python test-set=1 order=:log
from deephaven.systemic_obj_tracker import (
    is_systemic,
    set_systemic,
    is_systemic_object_marking_enabled,
)

if is_systemic_object_marking_enabled():
    set_systemic(True)
    print(is_systemic())
else:
    print("Systemic object marking is not currently enabled.")
```

The new status lasts until you change it again, so console code you run later on the same thread also uses it. The server runs console code from every session on the same thread, so the new status also applies to code that other sessions and clients run later.

To change the status only for a few lines, use a [context manager](#set-systemic-status-for-a-block-of-code) instead.

## Set systemic status for a block of code

The [`deephaven.systemic_obj_tracker`](/core/pydoc/code/deephaven.systemic_obj_tracker.html) module provides two context managers that set the current thread's systemic status for the code inside a `with` block:

- [`systemic_object_marking`](/core/pydoc/code/deephaven.systemic_obj_tracker.html#deephaven.systemic_obj_tracker.systemic_object_marking) makes objects created in the block systemic.
- [`no_systemic_object_marking`](/core/pydoc/code/deephaven.systemic_obj_tracker.html#deephaven.systemic_obj_tracker.no_systemic_object_marking) makes objects created in the block non-systemic.

On exit, each context manager restores the thread's previous status.

Like [`set_systemic`](/core/pydoc/code/deephaven.systemic_obj_tracker.html#deephaven.systemic_obj_tracker.set_systemic), both context managers raise a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError) if systemic object marking is not enabled.

```python skip-test
from deephaven import empty_table
from deephaven.systemic_obj_tracker import (
    systemic_object_marking,
    no_systemic_object_marking,
)

# Create a systemic table
with systemic_object_marking():
    systemic_t = empty_table(1).update("Label = `I am systemic`")

# Create a non-systemic table
with no_systemic_object_marking():
    not_systemic_t = empty_table(1).update("Label = `I am not systemic`")
```

## Mark an existing table systemic

To make an existing table systemic, call its [`with_attributes`](../reference/table-operations/create/withAttributes.md) method with `{"SystemicTable": True}`. It returns a new, systemic table that shares the original's data. The original table stays as it was.

In this example, `systemic_t` is systemic, and `t` keeps the status it had when it was created:

```python order=systemic_t,t
from deephaven import empty_table

t = empty_table(1).update("Label = `I am not systemic`")
systemic_t = t.with_attributes({"SystemicTable": True})
```

Tables you create from `systemic_t` don't inherit its mark. They take their status from the thread that creates them.

## Systemic scripts from the Python client

When you run a script on the server from the [Python client](../getting-started/pyclient-quickstart.md), the `systemic` argument of [`Session.run_script`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.run_script) sets the systemic status of the thread that runs it:

- `True` makes the objects the script creates systemic.
- `False` makes them non-systemic.
- `None`, the default, runs the script without changing the thread's status. When marking is enabled, its objects are non-systemic unless earlier console code marked that thread systemic.

When marking is disabled, the server ignores the argument, and every table and [single-table listener](../how-to-guides/table-listeners-python.md) the script creates is systemic.

```python skip-test
from pydeephaven import Session

# Connect to a Deephaven server on localhost:10000 with anonymous authentication
session = Session()

critical_script = """
from deephaven import empty_table

t = empty_table(1).update("Label = `I am systemic`")
"""
session.run_script(critical_script, systemic=True)
```

## Related documentation

- [Live DAG](./dag.md)
- [Table types](./table-types.md)
- [Execution context](./execution-context.md)
- [Table listeners](../how-to-guides/table-listeners-python.md)
- [Partitioned tables](../how-to-guides/partitioned-tables.md)
- [Formulas](../how-to-guides/formulas.md)
- [Python client quickstart](../getting-started/pyclient-quickstart.md)
- [Configuration properties](../how-to-guides/configuration/configuration-properties.md)
- [Configuration file](../how-to-guides/configuration/config-file.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`with_attributes`](../reference/table-operations/create/withAttributes.md)
- [`systemic_obj_tracker` Pydoc](/core/pydoc/code/deephaven.systemic_obj_tracker.html)
- [`merged_listen` Pydoc](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.merged_listen)
- [`DHError` Pydoc](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError)
- [`Session.run_script` Pydoc](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.run_script)
