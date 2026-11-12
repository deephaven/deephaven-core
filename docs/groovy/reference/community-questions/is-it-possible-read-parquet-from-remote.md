---
title: Is it possible to read a Parquet file from a remote location?
---

Yes. In addition to your local filesystem, Deephaven can read Parquet files directly from AWS S3, from S3-compatible object stores such as MinIO, and from Google Cloud Storage (`gs://` URIs). See [Import Parquet files](../../how-to-guides/data-import-export/parquet-import.md#from-s3) for examples that use `ParquetTools.readTable` with `S3Instructions`.

For other remote storage, you can try mounting it (for example, with a network or FUSE filesystem) so the files appear on a local path, but Deephaven doesn't provide or support that setup.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
