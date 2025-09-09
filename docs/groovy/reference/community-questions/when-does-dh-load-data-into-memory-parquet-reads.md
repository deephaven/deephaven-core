---
title: When does Deephaven load data into memory for Parquet reads?
sidebar_label: When does Deephaven load data into memory for Parquet reads?
---

<em>I know that when Deephaven loads a Parquet file into a table, it doesn't merge the entire file into memory. Is this still true if we merge the loaded data with, say, Kafka streaming data? What effect does performing an aggregation like `last_by` on the merged table have on memory use?</em>

<p></p>

When working with columnar sources like Parquet, Deephaven only reads the data it needs to. This is still true when merging, joining, or aggregating.

In the process of reading Parquet files, Deephaven undergoes several distinct phases:

1. _Discovery and Schema Inference_: Deephaven initially identifies Parquet files based on the data's layout and infers a `TableDefinition` from a single file's schema if the user does not explicitly supply one.
2. _Partitioning Column Filtering_: If applicable to the layout, Deephaven optimizes file selection by pruning files based on filters applied to partitioning columns, e.g. via `where` table operations.
3. _Metadata Consumption_: Deephaven consumes metadata (i.e. `metadata` and `common_metadata` files, or the footers of all Parquet files in the filtered set) to determine the distribution of rows across files and row groups to apply table operations that consume data or row count.
4. _Column Data Retrieval_: Accessing specific columns triggers Deephaven to read the corresponding data pages containing the required rows. These data pages are cached in memory within a heap-usage-sensitive cache.

To illustrate these phases in action, consider a scenario involving filtering on partitioning columns, merging with another table, and performing an aggregation:

- Deephaven initially prunes files based on partitioning column filters.
- Subsequently, it reads footer metadata for the remaining files.
- It then proceeds to read and cache the columns accessed by the remaining filters in the unpruned files.
- During the merge operation, no data is read.
- Finally, Deephaven reads all pages containing rows not filtered out in the previous step for the columns involved in the aggregation. For instance, if aggregating by columns "A" and "B" while computing max("C") and last("D"), only columns "A", "B", and "C" are read. Under certain circumstances, such as in ticking append-only or blink tables, column "D" may also be read, but only for the actual "last" row in each bucket.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
