//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Basic credentials that uses access key id and secret access key provided at construction.
 */
@Immutable
@SimpleStyle
abstract class SessionCredentials implements AwsSdkV2Credentials {

    static SessionCredentials of(
            final String accessKeyId, final String secretAccessKey, final String sessionToken) {
        return ImmutableSessionCredentials.of(accessKeyId, secretAccessKey, sessionToken);
    }

    @Value.Parameter
    abstract String accessKeyId();

    @Value.Redacted
    @Value.Parameter
    abstract String secretAccessKey();

    @Value.Redacted
    @Value.Parameter
    abstract String sessionToken();

    @Override
    public final AwsCredentialsProvider awsV2CredentialsProvider(@NotNull final S3Instructions instructions) {
        return StaticCredentialsProvider.create(AwsSessionCredentials.create(
                accessKeyId(), secretAccessKey(), sessionToken()));
    }
}
