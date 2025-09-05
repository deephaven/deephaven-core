---
title: What is an UpdateGraphConflictException and how do I fix it?
sidebar_label: Why am I getting an UpdateGraphConflictException?
---

_My code is throwing an `UpdateGraphConflictException`, but I don't know what that means or why it's happening. Why does this happen and how can I fix it?_

There are primarily two ways this exception can occur.

## 1. You are running the engine outside of a Deephaven server.

The Deephaven engine can be run outside of the context of a Deephaven server, such as in a standalone Java application. When run this way, some additional work is required to ensure that the current thread is aware of the necessary engine state when constructing a refreshing/ticking table. If you don't do this, you will likely encounter an [`UpdateGraphConflictException`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/exceptions/UpdateGraphConflictException.html).

Consider a [`TablePublisher`](https://docs.deephaven.io/core/javadoc/io/deephaven/stream/TablePublisher.html), which produces a blink table from added tables. This is a common way to construct a ticking table from a variety of streaming data sources. When run from a Deephaven server, the necessary engine state is set up automatically. However, run outside of the server, this work must be done manually.

The code below constructs an [execution context](../../conceptual/execution-context.md) in such a way that it is aware of the update graph. The update graph is passed into the [`TablePublisher`](https://docs.deephaven.io/core/javadoc/io/deephaven/stream/TablePublisher.html) constructor, which is necessary to ensure that the publisher can properly refresh the table when new data is available.

```java skip-test
final UpdateGraph tableUpdateGraph = PeriodicUpdateGraph.newBuilder("symbolIndexGraph").build();

final ExecutionContext dhEngineContext = ExecutionContext.newBuilder()
    .setUpdateGraph(tableUpdateGraph)
    .markSystemic() // this tells the engine this context is "system-wide" instead of "user" specific
    .build();

// On any thread you want to perform DH table operations needs to have the execution context set; it's ThreadLocal state.
dhEngineContext.open();

try {
    // Should now be able to create a table publisher.
    final TablePublisher tablePublisherSymbolIndex = TablePublisher.of(
            TABLE_SYMBOL_INDEX,
            tableDefinition,
            tp -> new AtomicInteger().getAndIncrement(),
            () -> new MutableBoolean().setValue(true),
            tableUpdateGraph,
            2048
    );
} finally {
    dhEngineContext.close();
}
```

## 2. You try to combine ticking tables from separate update graphs.

Consider the following scenario, with two separate update graphs:

- Update graph A
  - Ticking table A
- Update graph B
  - Ticking table B

You want to combine these two ticking tables into a single table. Any attempt to do so will result in an `UpdateGraphConflictException`, because the two ticking tables are from different update graphs. The resultant table cannot possibly be consistent - the resultant table cannot live in either existing update graph, and it cannot be in a new update graph that is not the same as either of the existing graphs.

The only way to combine these tables is to take a snapshot of each to produce a static table, then combine the static tables.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not answered in our documentation, [join our Community](/slack) and ask it there. We are happy to help!
