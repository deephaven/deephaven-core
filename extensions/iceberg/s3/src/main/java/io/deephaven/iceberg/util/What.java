//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Map;

public class What implements AwsCredentialsProvider {

    public static AwsCredentialsProvider create(Map<String, String> properties) {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(
                properties.get(S3FileIOProperties.ACCESS_KEY_ID),
                properties.get(S3FileIOProperties.SECRET_ACCESS_KEY)));
    }

    private What() {}

    @Override
    public AwsCredentials resolveCredentials() {
        throw new UnsupportedOperationException();
    }
}
