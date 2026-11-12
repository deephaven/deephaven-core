---
id: message-larger-than-max
title: How do I fix client errors saying "Received message larger than max"?
---

_I have a client query that gives an error `Received message larger than max`. How can I fix this?_

The Deephaven server and the Java client each accept messages up to 100 MiB by default. [gRPC's](https://grpc.io) default receive limit is 4 MiB. When a message exceeds the receiving side's limit, the receiver fails with `Received message larger than max`.

To fix this, raise the limit on whichever side reports the error:

### Java client

For the Java client (usable from Groovy), set the limit with the [`maxInboundMessageSize`](/core/javadoc/io/deephaven/client/impl/ClientConfig.html#maxInboundMessageSize()) method of the `ClientConfig` builder, then use that config to create the session:

```groovy skip-test
import io.deephaven.client.impl.ClientConfig
import io.deephaven.uri.DeephavenTarget

def target = DeephavenTarget.of(URI.create("dh://localhost:10000"))
def clientConfig = ClientConfig.builder()
    .target(target)
    .maxInboundMessageSize(200 * 1024 * 1024) // 200 MiB
    .build()
```

### Server

Set the `grpc.maxInboundMessageSize` [configuration property](../../how-to-guides/configuration/configuration-properties.md) (in bytes). For example, to raise it to 200 MiB:

```bash
-Dgrpc.maxInboundMessageSize=209715200
```

### Alternative: fetch less data

Instead of raising the limit, you can reduce message sizes by filtering data or selecting fewer columns before fetching. For example, apply `.where()` or `.select()` on the server side so each message carries less data.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
