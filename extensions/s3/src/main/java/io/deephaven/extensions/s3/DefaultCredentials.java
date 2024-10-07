//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * Default credentials provider used by AWS SDK that looks for credentials at a number of locations as described in
 * {@link DefaultCredentialsProvider}
 */
enum DefaultCredentials implements AwsSdkV2Credentials {
    INSTANCE;

    @Override
    public final AwsCredentialsProvider awsV2CredentialsProvider(@NotNull final S3Instructions instructions) {
        return DefaultCredentialsProvider.create();
    }
}
