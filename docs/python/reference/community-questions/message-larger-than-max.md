---
title: How do I fix client errors saying "Received message larger than max"?
sidebar_label: How do I increase the maximum message size?
---

_I have a client query that gives an error `Received message larger than max`. How can I fix this?_

[gRPC](https://grpc.io) has a default maximum message size of 4MB, but Deephaven clients have a default maximum size of 100MB. Attempting to send messages larger than 100MB will result in an error. To fix this, you should either:

- Reduce message sizes by breaking them into smaller chunks.
- Increase the maximum message size on both the client and server.

For the latter, refer to [`Grpc_arg_keys`](https://grpc.github.io/grpc/core/group__grpc__arg__keys.html) for available options. The relevant keys are:

- `GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH`
- `GRPC_ARG_MAX_SEND_MESSAGE_LENGTH`

For the Python client, you can set these options when constructing the session via the `client_opts` parameter:

```python skip-test
import pydeephaven

session = pydeephaven.Session(
    host="localhost",
    port=10000,
    client_opts=[
        ("grpc.max_receive_message_length", 10 * 1024 * 1024),  # 10 MB
        ("grpc.max_send_message_length", 10 * 1024 * 1024),  # 10 MB
    ],
)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
