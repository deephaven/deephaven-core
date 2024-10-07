//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

enum AnonymousCredentials implements AwsSdkV2Credentials {
    ANONYMOUS_CREDENTIALS;

    @Override
    public AwsCredentialsProvider awsV2CredentialsProvider(@NotNull final S3Instructions instructions) {
        return AnonymousCredentialsProvider.create();
    }
}
