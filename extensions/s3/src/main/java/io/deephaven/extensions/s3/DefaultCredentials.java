//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * Default AWS credentials provider that looks for credentials at a number of locations as described in
 * {@link DefaultCredentialsProvider}
 */
enum DefaultCredentials implements AwsSdkV2Credentials {
    DEFAULT_CREDENTIALS;

    @Override
    public final AwsCredentialsProvider awsV2CredentialsProvider() {
        return DefaultCredentialsProvider.create();
    }
}
