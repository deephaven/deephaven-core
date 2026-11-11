---
title: How can I assume a specific role for S3 access?
---

_I need to assume an IAM role to access S3, or I only have temporary credentials. How can I use these in Deephaven?_

If you already have an access key, secret key, and session token (for example, from [AWS STS](https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html)), pass them directly with [`Credentials.session`](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.Credentials.session):

```python skip-test
from deephaven.experimental.s3 import S3Instructions, Credentials

s3_instructions = S3Instructions(
    region_name="us-east-1",
    credentials=Credentials.session(
        access_key_id="AKIA...",
        secret_access_key="...",
        session_token="...",
    ),
)
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

`SomeSpecificRole` uses the `SomeUser` credentials to [assume an IAM role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html). Assuming a role returns temporary credentials that carry that role's permissions. The Deephaven server must be able to read the config file — at its default location, `~/.aws/config`, for the user running the server, or at the path you pass to `config_file_path`. In Docker, this means mounting the file into the container.

With this configuration, pass the role profile's name to the `profile_name` parameter of [`S3Instructions`](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions):

```python skip-test
from deephaven.experimental.s3 import S3Instructions

# Ignoring other input parameters for brevity
s3_instructions = S3Instructions(
    profile_name="SomeSpecificRole",
)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
