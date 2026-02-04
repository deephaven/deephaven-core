---
title: What's the best approach to monitor Deephaven server health?
sidebar_label: What's the best approach to monitor Deephaven server health?
---

- The default Deephaven server Docker image has a [built-in healthcheck](https://github.com/deephaven/deephaven-core/blob/main/docker/server/src/main/docker/Dockerfile#L31).
- A better option would be to use a GRPC health probe. For more information on that, see [GRPC health checking protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md).
- Another option is to ingest [performance tables](../../how-to-guides/performance/performance-tables.md) into a client API and monitor them there.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
