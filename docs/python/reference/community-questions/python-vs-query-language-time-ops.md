---
title: Why aren't my date-time table operations working as expected?
---

<em>I'm trying to do downstream calculations on a date-time column, but everything I try throws an error. What's going on?</em>

This usually happens when a column has the type `PyObject` or `Object` instead of a proper date-time type. For example, calling a Python function with no return type hint in a query string produces a `PyObject` column, because the engine cannot infer the column type:

```python test-set=1 order=source
from deephaven.time import to_j_instant
from deephaven import empty_table


def make_ts():
    return to_j_instant("2024-01-01T00:00:00 ET")


source = empty_table(1).update(["Timestamp = make_ts()"])
```

The `Timestamp` column is now a `PyObject`. Downstream operations that expect a typed column fail:

```python test-set=1 should-fail
result = source.update(["MillisSinceEpoch = epochMillis(Timestamp)"])
```

The error occurs because the query language has no `epochMillis` overload that accepts a `PyObject`.

The simplest fix is to use the built-in [`DateTimeUtils`](/core/javadoc/io/deephaven/time/DateTimeUtils.html) methods in query strings instead of Python functions. These methods run in Java without crossing into Python, and they return properly typed columns. `DateTimeUtils` is auto-imported into the query language, so no imports are needed:

```python test-set=1 order=result,source
from deephaven import empty_table

source = empty_table(1).update(["Timestamp = parseInstant(`2024-01-01T00:00:00 ET`)"])
result = source.update(["MillisSinceEpoch = epochMillis(Timestamp)"])
```

If you must call a Python function in a query string, add a return type hint so the engine can infer the column type. For example, `to_j_instant` has a return type of `Instant`, so calling it directly (rather than wrapping it in an unannotated function) produces a properly typed column. The same applies to any Python function called in a query string: prefer a built-in query-language method, and when you need Python, add a return type hint.

For more information on this topic, see the following guides:

- [Time in Deephaven](../../conceptual/time-in-deephaven.md)
- [The Python-Java boundary](../../conceptual/python-java-boundary.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
