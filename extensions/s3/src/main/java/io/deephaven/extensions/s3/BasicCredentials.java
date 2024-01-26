/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * AWS credentials provider that uses access key and secret key provided at construction.
 */
@Immutable
@SimpleStyle
abstract class BasicCredentials implements AwsSdkV2Credentials {

    static BasicCredentials of(final String awsAccessKeyId, final String awsSecretAccessKey) {
        return ImmutableBasicCredentials.of(awsAccessKeyId, awsSecretAccessKey);
    }

    @Value.Parameter
    abstract String awsAccessKeyId();

    @Value.Redacted
    @Value.Parameter
    abstract String awsSecretAccessKey();

    public AwsCredentialsProvider awsCredentialsProvider() {
        final AwsBasicCredentials awsCreds = AwsBasicCredentials.create(awsAccessKeyId(), awsSecretAccessKey());
        return StaticCredentialsProvider.create(awsCreds);
    }
}
