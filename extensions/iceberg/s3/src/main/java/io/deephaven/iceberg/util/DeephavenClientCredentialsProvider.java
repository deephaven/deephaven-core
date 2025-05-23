//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

/**
 * {@link AwsCredentialsProvider} implementation used for configuring credentials for S3 clients when working with
 * Iceberg. This provider emulates the behavior of {@link S3FileIOProperties#applyCredentialConfigurations} and is added
 * as a workaround till the issue <a href="https://github.com/apache/iceberg/issues/13131">Iceberg Issue #13131</a> is
 * fixed. So should be removed once the issue is fixed.
 */
public class DeephavenClientCredentialsProvider implements AwsCredentialsProvider {

    private final S3FileIOProperties s3FileIOProperties;
    private final AwsClientProperties awsClientProperties;

    private AwsCredentials cachedCredentials;

    public static DeephavenClientCredentialsProvider create(Map<String, String> properties) {
        return new DeephavenClientCredentialsProvider(properties);
    }

    private DeephavenClientCredentialsProvider(@NotNull final Map<String, String> properties) {
        this.s3FileIOProperties = new S3FileIOProperties(properties);
        this.awsClientProperties = new AwsClientProperties(properties);
    }

    @Override
    public AwsCredentials resolveCredentials() {
        if (cachedCredentials == null) {
            final AwsCredentialsProvider delegateCredentialsProvider =
                    s3FileIOProperties.isRemoteSigningEnabled()
                            ? AnonymousCredentialsProvider.create()
                            : awsClientProperties.credentialsProvider(s3FileIOProperties.accessKeyId(),
                                    s3FileIOProperties.secretAccessKey(), s3FileIOProperties.sessionToken());
            cachedCredentials = delegateCredentialsProvider.resolveCredentials();
        }
        return cachedCredentials;
    }
}
