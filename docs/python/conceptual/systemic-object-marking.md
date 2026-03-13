---
title: Systemic Object Marking
sidebar_label: Systemic Object Marking
---

In Deephaven, a **systemic object** is an object of critical importance—such as a table essential for reliable data analysis. If a systemic object fails, Deephaven terminates the entire worker process to maintain data integrity. By default, any object created on a thread is marked as systemic, providing maximum protection for your data flow. This guide explains how to manage this behavior using the [`deephaven.systemic_obj_tracker`](https://docs.deephaven.io/core/pydoc/code/deephaven.systemic_obj_tracker.html) module.

## Enable Systemic Object Marking

Systemic object marking is not user-configurable at runtime by default. To enable this feature, start the server with the JVM option:

```
-DSystemicObjectTracker.enabled=true
```

To check if systemic object marking is enabled:

```python test-set=1 order=null
from deephaven.systemic_obj_tracker import is_systemic_object_marking_enabled

print(is_systemic_object_marking_enabled())
```

## Manage Thread Systemic Status

Systemic object creation is controlled per thread. When systemic object marking is enabled, you can programmatically set a thread as systemic or non-systemic at runtime. While a thread is systemic, any object it creates is also systemic; when non-systemic, objects are not marked as systemic.

To check and toggle the systemic status of the current thread:

```python test-set=1 order=null
from deephaven.systemic_obj_tracker import is_systemic, set_systemic

if is_systemic_object_marking_enabled():
    set_systemic(not is_systemic())
else:
    print("Systemic object marking is not currently enabled.")
```

## Context Managers

The [`deephaven.systemic_obj_tracker`](https://docs.deephaven.io/core/pydoc/code/deephaven.systemic_obj_tracker.html) module provides context managers to enable or disable systemic object creation within code blocks.

```python skip-test
from deephaven import empty_table
from deephaven.systemic_obj_tracker import (
    systemic_object_marking,
    no_systemic_object_marking,
)

# Create a systemic table
with systemic_object_marking():
    systemic_t = empty_table(1).update("Label=`I am systemic`")

# Create a non-systemic table
with no_systemic_object_marking():
    not_systemic_t = empty_table(1).update("Label=`I am not systemic`")
```

## Systemic scripts from the Python Client

When executing a script on the server from the Python client, you can control whether objects created by that script are marked as systemic by using the `systemic` argument—provided systemic object marking is enabled on the server. If systemic object marking is disabled on the server, the `systemic` argument has no effect, and all objects created by the script will be systemic by default.

```python skip-test
from pydeephaven import Session

# Deephaven server running on <localhost:10000> with 'Anonymous' authentication
session = Session()

critical_script = """
from deephaven import empty_table

t = empty_table(1).update("Label=`I am systemic`")
"""
session.run_script(critical_script, systemic=True)
```

## Related documentation

- [Server Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.systemic_obj_tracker.html)
- [Client Pydoc](https://docs.deephaven.io/core/client-api/python/code/pydeephaven.html#pydeephaven.Session.run_script)
