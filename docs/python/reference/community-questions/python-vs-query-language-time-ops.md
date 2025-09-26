---
title: Why aren't my date-time table operations working as expected?
sidebar_label: Why aren't my date-time table operations working as expected?
---

<em>I'm trying to do downstream calculations on a date-time column, but everything I try throws an error. What's going on?</em>

This is a fairly common issue that users run into when writing queries in Python, and it's usually caused by the existence of a `PyObject` or `Object` column existing in the table. Take, for instance, the following query that creates a one-row table with a timestamp at midnight on New Years' Day of 2024:

```python test-set=1 order=source
from deephaven.time import to_j_instant
from deephaven import empty_table

source = empty_table(1).update(["Timestamp = to_j_instant(`2024-01-01T00:00:00 ET`)"])
```

That worked, so what's the issue? Well, trying a downstream operation on the `Timestamp` column doesn't work:

```python test-set=1 should-fail
result = source.update(["MillisSinceEpoch = epochMillis(Timestamp)"])
```

Here's part of the error:

```
r-Scheduler-Serial-1 | .c.ConsoleServiceGrpcImpl | Error running script: java.lang.RuntimeException: Error in Python interpreter:
Type: <class 'deephaven.dherror.DHError'>
Value: table update operation failed. : Cannot find method epochMillis(java.lang.Object)
```

This error is caused because there's no query language method `epochMillis` that exists for a `java.lang.Object` column type.

The fix for this is simple: do _not_ use methods found in [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) in query strings unless it's absolutely necessary. There are a huge number of available methods in the [`DateTimeUtils`](/core/javadoc/io/deephaven/time/DateTimeUtils.html) Java class that can do the same thing. That class is auto-imported into the Deephaven Query Language on server startup, so you can call them with no imports at all.

```python test-set=1 order=result,source
from deephaven import empty_table

source = empty_table(1).update(["Timestamp = parseInstant(`2024-01-01T00:00:00 ET`)"])
result = source.update(["MillisSinceEpoch = epochMillis(Timestamp)"])
```

This principle is true for operations outside of date-time libraries as well. For more information on this topic, see the following guides:

- [Time in Deephaven](../../conceptual/time-in-deephaven.md)
- [The Python-Java boundary](../../conceptual/python-java-boundary.md)

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
