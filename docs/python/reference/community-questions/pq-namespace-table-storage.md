---
title: What namespace do system tables created by a PQ (Persistent Query) get put in?
sidebar_label: What namespace do system tables created by a PQ get put in?
---

<em>What namespace is a new PQ created in? Or, what namespace do system tables created by a PQ get put in?</em>

<p></p>

This is a great question that touches on several different concepts in Deephaven. Let me clarify the different scenarios:

## 1. PQ (Persistent Query) Namespace

**Persistent Queries themselves do not have a namespace.** A PQ is a query execution mechanism, not a table storage location. However, any system tables that a PQ creates or operates on will be stored according to the rules below.

## 2. Deephaven Community - Table Storage Location

In **Deephaven Community**, you specify the location where tables are stored when using data export methods:

- **Parquet files**: Use `parquet.write()`, `parquet.write_partitioned()`, or `parquet.batch_write()` with a specified `path` parameter (local filesystem or S3)
- **CSV files**: Use `write_csv()` with a specified destination path
- **Other formats**: Various export functions allow you to specify the output location

Example:

```python syntax
from deephaven import parquet

# You specify exactly where the table gets written
parquet.write(table=my_table, path="/data/output/my_table.parquet")

# For S3 storage
from deephaven.experimental import s3

credentials = s3.Credentials.basic(
    access_key_id="your_access_key", secret_access_key="your_secret_key"
)

parquet.write(
    table=my_table,
    path="s3://your-bucket/my_table.parquet",
    special_instructions=s3.S3Instructions(
        region_name="us-east-1",
        credentials=credentials,
    ),
)
```

## 3. Deephaven Enterprise - Merge Query Output Location

In **Deephaven Enterprise merge queries**, you can specify the output location using the merge settings:

- **Namespace**: Specify the target namespace for the merged data
- **Table**: Specify the target table name within that namespace

The merge query configuration includes:

- `Namespace`: The namespace where the merged data will be stored
- `Table`: The table name for the merged data
- Additional settings for partitioning, format, and compression

## 4. Deephaven Enterprise - User Table Saving

In **Deephaven Enterprise regular queries**, when you save user tables using the Database APIs, you specify both the namespace and table name:

- **System namespaces**: Follow structured administrative processes and are typically managed by administrators
- **User namespaces**: Managed directly by users via the Database APIs with limited privileges

The format is typically: `UserNamespace.TableName`

For example, when deleting a user table, you would reference it as:

```bash
dhconfig schemas delete UserNamespace.TableName --force
```

## Summary

- **PQ itself**: No namespace (it's a query execution mechanism)
- **Community**: You specify the file path/location when exporting
- **Enterprise merge queries**: You specify the namespace and table in merge settings
- **Enterprise user tables**: Stored in user-specified namespaces via Database APIs

For more specific details about Enterprise merge queries and user table management, I'd recommend consulting the Enterprise documentation or contacting Deephaven support, as these features have additional configuration options and administrative considerations.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
