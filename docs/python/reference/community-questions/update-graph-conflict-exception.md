---
title: What is an UpdateGraphConflictException and how do I fix it?
---

_My code is throwing an `UpdateGraphConflictException`, but I don't know what that means or why it's happening. Why does this happen and how can I fix it?_

An [`UpdateGraphConflictException`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/exceptions/UpdateGraphConflictException.html) has two primary causes. Each cause produces a different error message, so check the message to identify which applies.

## 1. Running the engine outside a Deephaven server

> **Error message:** `Table was constructed with a static-only UpdateGraph.`

This applies to Java applications that embed the Deephaven engine directly. Python code running in a Deephaven server or console already has an [update graph](../../conceptual/dag.md#update-graph-cycles) in its [execution context](../../conceptual/execution-context.md).

The Deephaven engine can run outside of a Deephaven server, such as in a standalone Java application. When run this way, the current thread must have an execution context that holds an [update graph](../../conceptual/dag.md#update-graph-cycles) before it constructs a refreshing (ticking) table. An update graph is the engine component that processes each ticking table's changes in cycles. If the thread has no real update graph, the table is created against a static-only placeholder and the engine throws an `UpdateGraphConflictException`.

Consider a [`TablePublisher`](https://docs.deephaven.io/core/javadoc/io/deephaven/stream/TablePublisher.html), which produces a [blink table](../../conceptual/table-types.md#specialization-3-blink) from added tables. This is a common way to construct a ticking table from a variety of streaming data sources. When run from a Deephaven server, the necessary engine state is set up automatically. However, run outside of the server, this work must be done manually.

The code below constructs an [execution context](../../conceptual/execution-context.md) with an update graph and opens it on the current thread. The update graph is passed to `TablePublisher.of`, which is necessary to ensure that the publisher can properly refresh the table when new data is available.

> [!NOTE]
> This context sets only the update graph. Operations that compile formulas — such as `update`, `where` with a string filter, and so on — also need a `QueryLibrary`, `QueryScope`, and `QueryCompiler`. See the [execution context](../../conceptual/execution-context.md) page for details.

```java skip-test
final UpdateGraph tableUpdateGraph = PeriodicUpdateGraph.newBuilder("symbolIndexGraph").build();
// The application must call tableUpdateGraph.start() before the blink table will tick.

final ExecutionContext dhEngineContext = ExecutionContext.newBuilder()
    .setUpdateGraph(tableUpdateGraph)
    .markSystemic()
    .build();

// Every thread that performs Deephaven table operations must have this execution context open.
// open() returns a SafeCloseable that restores the previous context when closed.
try (final SafeCloseable ignored = dhEngineContext.open()) {
    final TableDefinition definition = TableDefinition.of(
            ColumnDefinition.ofString("Symbol"),
            ColumnDefinition.ofDouble("Price"));

    final TablePublisher publisher = TablePublisher.of(
            "SymbolIndex",
            definition,
            null,  // optional flush callback
            null,  // optional shutdown callback
            tableUpdateGraph,
            2048
    );
}
```

## 2. Combining ticking tables from separate update graphs

> **Error message:** `Multiple update graphs found in dependencies`

Every ticking table belongs to exactly one [update graph](../../conceptual/dag.md#update-graph-cycles), which processes that table's changes. Consider the following scenario, with two separate update graphs:

- Update graph A
  - Ticking table A
- Update graph B
  - Ticking table B

You want to combine these two ticking tables into a single table. Each update graph runs its own update cycles, so no single cycle could process changes from both tables consistently. When an operation has refreshing inputs from more than one update graph, the engine throws an [`UpdateGraphConflictException`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/exceptions/UpdateGraphConflictException.html) with the message "Multiple update graphs found in dependencies". Static (non-refreshing) tables are not checked.

Because static tables are exempt from this check, you can [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) each table to produce a static copy, then combine the static results.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not answered in our documentation, [join our Community](/slack) and ask it there. We are happy to help!
