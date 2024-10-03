//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Basic credentials that uses access key id and secret access key provided at construction.
 */
@Immutable
@SimpleStyle
abstract class BasicCredentials implements AwsSdkV2Credentials {

    static BasicCredentials of(final String accessKeyId, final String secretAccessKey) {
        return ImmutableBasicCredentials.of(accessKeyId, secretAccessKey);
    }

    @Value.Parameter
    abstract String accessKeyId();

    @Value.Redacted
    @Value.Parameter
    abstract String secretAccessKey();

    @Override
    public final AwsCredentialsProvider awsV2CredentialsProvider(@NotNull final S3Instructions instructions) {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId(), secretAccessKey()));
    }
}
