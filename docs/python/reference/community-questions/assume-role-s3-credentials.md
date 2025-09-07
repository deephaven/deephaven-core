---
title: How can I assume a specific role for S3 access?
sidebar_label: How can I assume a specific role for S3?
---

_I only have access to S3 via temporary credentials. How can I use these in Deephaven?_

<!-- TODO: Update this page to link to `SessionCredentials` once 0.40 goes out. -->

One way to use temporary credentials in Deephaven is to configure the AWS SDK to use them. Consider an AWS credentials file with a user and a specific role:

```conf
[profile SomeUser]
# SomeUser is configured with access keys, SSO, or other credentials

[profile SomeSpecificRole]
role_arn = ...
region = ...
source_profile = SomeUser
```

`SomeSpecificRole` uses `SomeUser` to assume a role. With this configuration, you can use the appropriate profile as input to an [`S3Instructions`](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.s3.html#deephaven.experimental.s3.S3Instructions) object like this:

```python skip-test
from deephaven.experimental.s3 import S3Instructions

# Ignoring other input parameters for brevity
s3_instructions = S3Instructions(
    profile_name="SomeSpecificRole",
)
```

This configuration uses [AWS Security Token Service (STS)](https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html) to assume a role and obtain temporary credentials.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
