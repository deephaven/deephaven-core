//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * Default AWS credentials provider that looks for credentials at a number of locations as described in
 * {@link DefaultCredentialsProvider} and falls back to anonymous credentials if no credentials are found.
 */
enum DefaultCredentials implements AwsSdkV2Credentials, AwsCredentialsProvider {
    DEFAULT_CREDENTIALS;

    private static final AwsCredentialsProviderChain PROVIDER_CHAIN = AwsCredentialsProviderChain.builder()
            .reuseLastProviderEnabled(true)
            .credentialsProviders(new AwsCredentialsProvider[] {
                    DefaultCredentialsProvider.create(),
                    AnonymousCredentialsProvider.create()
            })
            .build();

    @Override
    public final AwsCredentialsProvider awsV2CredentialsProvider() {
        return DEFAULT_CREDENTIALS;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return PROVIDER_CHAIN.resolveCredentials();
    }
}
