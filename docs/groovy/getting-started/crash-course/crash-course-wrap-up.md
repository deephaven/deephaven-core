---
title: Wrapping up
---

## What you learned

This wraps up the Deephaven crash course. In it, you learned about:

- Deephaven's basic architecture
- Creating tables
- Table operations
- Time and calendars
- Query strings
- Recipes, not loops
- Plotting
- Data I/O
- Configuring your Deephaven instance

## Where to go from here

This crash course is a starting point. To continue learning about Deephaven, check out any of these links, grouped by the crash course topic they build on, followed by the API reference:

- Deephaven's basic architecture
  - [Deephaven's design](../../conceptual/deephaven-design.md)
  - [Incremental update model](../../conceptual/table-update-model.md)
  - [Live directed acyclic graph (DAG)](../../conceptual/dag.md)
  - [Table listeners](../../how-to-guides/table-listeners-groovy.md)
- Creating tables
  - [Replay data from static tables](../../how-to-guides/replay-data.md)
- Table operations
  - [Access table metadata](../../how-to-guides/metadata.md)
  - [Select and create columns](../../how-to-guides/use-select-view-update.md)
  - [Filter table data](../../how-to-guides/use-filters.md)
  - [Perform multiple aggregations at once](../../how-to-guides/combined-aggregations.md)
  - [Group and ungroup data](../../how-to-guides/grouping-data.md)
  - [Compute rolling aggregations with `updateBy`](../../how-to-guides/rolling-aggregations.md)
  - [Perform exact and relational joins](../../how-to-guides/joins-exact-relational.md)
  - [Perform inexact, time-series, and range joins](../../how-to-guides/joins-timeseries-range.md)
  - [Create and use partitioned tables](../../how-to-guides/partitioned-tables.md)
  - [Capture table history with snapshots](../../how-to-guides/capture-table-history.md)
  - [Query Deephaven tables with SQL](../../how-to-guides/data-import-export/execute-sql-queries.md)
- Time and calendars
  - [Work with time](../../conceptual/time-in-deephaven.md)
  - [Work with calendars](../../how-to-guides/business-calendar.md)
- Query strings
  - [Query string overview](../../how-to-guides/query-string-overview.md)
  - [Groovy variables in query strings](../../how-to-guides/groovy-variables.md)
  - [User-defined functions](../../how-to-guides/groovy-closures.md)
  - [Groovy classes and objects in query strings](../../how-to-guides/groovy-classes.md)
  - [Programmatic query string generation](../../how-to-guides/generate-query-strings.md)
- Recipes, not loops
  - [Vectorization and the recipe paradigm](../../conceptual/vectorization-and-recipes.md)
- Plotting
  - [Built-in plotting API](../../how-to-guides/plotting/api-plotting.md)
- Data I/O
  - [Read CSV files](../../how-to-guides/data-import-export/csv-import.md)
  - [Read Parquet files](../../how-to-guides/data-import-export/parquet-import.md)
  - [Connect to a Kafka stream](../../how-to-guides/data-import-export/kafka-stream.md)
  - [Work with Iceberg tables](../../how-to-guides/data-import-export/iceberg.md)
- Configuring your Deephaven instance
  - [Create a Deephaven configuration file](../../how-to-guides/configuration/config-file.md)
  - [Update Deephaven](../../how-to-guides/configuration/updating-deephaven.md)
- API reference
  - [Java server API documentation (Javadoc)](/core/javadoc/)

## Get help

Learning Deephaven can be challenging at times, and we want you to succeed. These resources can help:

- The [Deephaven overview](../../conceptual/deephaven-overview.md) explains what Deephaven is and how it works.
- The [community questions](../../reference/community-questions/cq-index.md) cover commonly asked Deephaven questions.
- We encourage you to join our [community Slack channel](/slack), where we're happy to support you through your Deephaven journey and answer any questions you may have.
