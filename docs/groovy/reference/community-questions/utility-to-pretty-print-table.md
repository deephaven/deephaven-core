---
title: Is there a utility to pretty-print a table to the log?
---

Yes. [`TableTools.show(table)`](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#show(io.deephaven.engine.table.Table,java.lang.String...)) prints the first 10 rows of a table to standard output, which appears in the IDE's **Log** panel and the server log. To print more rows, pass a row count, for example `show(table, 100)`. To get the formatted text as a `String`, for example to pass to your own logger, use [`TableTools.string(table)`](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#string(io.deephaven.engine.table.Table,java.lang.String...)). `string` also defaults to 10 rows; use `string(table, 100)` for more.

`TableTools` is statically imported in the Groovy console, so `show` and `string` can be called directly:

```groovy
t = emptyTable(20).update("X = i")
show(t)
show(t, 20)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
