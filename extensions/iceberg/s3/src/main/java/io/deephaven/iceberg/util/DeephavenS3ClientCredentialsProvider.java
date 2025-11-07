//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

/**
 * {@link AwsCredentialsProvider} implementation used for configuring credentials for S3 clients when working with
 * Iceberg. This provider emulates the behavior of {@link S3FileIOProperties#applyCredentialConfigurations} and is added
 * as a workaround till the issue <a href="https://github.com/apache/iceberg/issues/13131">Iceberg Issue #13131</a> is
 * fixed.
 */
// TODO (DH-19448): Remove this class once Iceberg fix for #13131 is released
public final class DeephavenS3ClientCredentialsProvider implements AwsCredentialsProvider {

    // See org.apache.iceberg.aws.AwsClientProperties.credentialsProvider
    // It does do a check to make sure the class implements AwsCredentials provider, but that isn't actually necessary
    // since it will just take the return value of this create.
    @SuppressWarnings("unused")
    public static AwsCredentialsProvider create(final Map<String, String> properties) {
        final S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(properties);
        final AwsClientProperties awsClientProperties = new AwsClientProperties(properties);
        return s3FileIOProperties.isRemoteSigningEnabled()
                ? AnonymousCredentialsProvider.create()
                : awsClientProperties.credentialsProvider(s3FileIOProperties.accessKeyId(),
                        s3FileIOProperties.secretAccessKey(), s3FileIOProperties.sessionToken());
    }

    private DeephavenS3ClientCredentialsProvider() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AwsCredentials resolveCredentials() {
        throw new UnsupportedOperationException();
    }
}
