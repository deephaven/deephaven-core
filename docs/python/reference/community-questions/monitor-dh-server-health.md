---
title: What's the best approach to monitor Deephaven server health?
---

Use the server's built-in gRPC health service for liveness checks, and the performance tables for deeper monitoring.

- The Deephaven server implements the standard [gRPC health checking protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md). The [published Docker images](https://github.com/deephaven/deephaven-server-docker/blob/main/contexts/server/Dockerfile) already include a `HEALTHCHECK` that runs `grpc_health_probe` against port 10000. Outside Docker, for example in a Kubernetes liveness or readiness probe, point any gRPC health probe at the server's port.
- To monitor query and update-graph performance rather than just liveness, use a client API (for example, `pydeephaven` or the Java client) to connect to the server and subscribe to its [performance tables](../../how-to-guides/performance/performance-tables.md).

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
