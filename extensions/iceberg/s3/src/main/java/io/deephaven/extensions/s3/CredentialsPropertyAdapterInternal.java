//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.Map;

public final class CredentialsPropertyAdapterInternal {

    public static void adapt(Credentials credentials, Map<String, String> propertiesOut) {
        if (credentials instanceof BasicCredentials) {
            propertiesOut.put(S3FileIOProperties.ACCESS_KEY_ID, ((BasicCredentials) credentials).accessKeyId());
            propertiesOut.put(S3FileIOProperties.SECRET_ACCESS_KEY, ((BasicCredentials) credentials).secretAccessKey());
        } else if (credentials == Credentials.anonymous()) {
            propertiesOut.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                    AnonymousCredentialsProvider.class.getName());
        } else if (credentials == Credentials.defaultCredentials()) {
            propertiesOut.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                    DefaultCredentialsProvider.class.getName());
        } else {
            throw new IllegalStateException("Unexpected credentials: " + credentials);
        }
    }
}
