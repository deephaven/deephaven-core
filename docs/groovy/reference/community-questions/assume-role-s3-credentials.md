---
title: How can I assume a specific role for S3 access?
---

_I need to assume an IAM role to access S3, or I only have temporary credentials. How can I use these in Deephaven?_

If you already have an access key, secret key, and session token (for example, from [AWS STS](https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html)), pass them directly with [`Credentials.session`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/s3/Credentials.html#session(java.lang.String,java.lang.String,java.lang.String)):

```groovy skip-test
import io.deephaven.extensions.s3.Credentials
import io.deephaven.extensions.s3.S3Instructions

s3Instructions = S3Instructions.builder()
    .regionName("us-east-1")
    .credentials(Credentials.session("AKIA...", "...", "..."))
    .build()
```

If you need to assume a role, configure an [AWS config file](https://docs.aws.amazon.com/sdkref/latest/guide/file-format.html) (`~/.aws/config`) with a user and a role profile instead:

```conf
[profile SomeUser]
# SomeUser is configured with access keys or other credentials

[profile SomeSpecificRole]
role_arn = ...
region = ...
source_profile = SomeUser
```

`SomeSpecificRole` uses the `SomeUser` credentials to [assume an IAM role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html). Assuming a role returns temporary credentials that carry that role's permissions. The Deephaven server must be able to read the config file — at its default location, `~/.aws/config`, for the user running the server, or at the path you pass to `configFilePath`. In Docker, this means mounting the file into the container.

With this configuration, pass the role profile's name to the `profileName` builder method of [`S3Instructions`](https://docs.deephaven.io/core/javadoc/io/deephaven/extensions/s3/S3Instructions.html):

```groovy skip-test
import io.deephaven.extensions.s3.S3Instructions

// Ignoring other input parameters for brevity
s3Instructions = S3Instructions.builder()
    .profileName("SomeSpecificRole")
    .build()
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
