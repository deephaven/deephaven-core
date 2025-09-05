---
title: Why can't I read from an S3 bucket but I can connect to it?
sidebar_label: Why can't I read from S3?
---

_I've got some trouble reading Parquet data from an S3 server. I can connect to the bucket, but all attempts to read data fail. Why is this happening?_

The most common cause for this issue is timeouts. Your failed reads produce a stack trace that should contain a message like:

`did not complete before the specified timeout`

This indicates that the operation takes longer than what is allowed. Deephaven's [`S3Instructions`](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) object allows you to specify a timeout for both reading from and writing to S3. You can increase the timeout by setting the `read_timeout` and `write_timeout` parameters to a higher value. The default for both is 2 seconds.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
