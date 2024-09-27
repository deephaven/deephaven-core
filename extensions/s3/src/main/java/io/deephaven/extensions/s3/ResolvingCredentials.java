//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

/**
 * Default credentials provider used by Deephaven which resolves credentials in the following order:
 * <ol>
 * <li>If a profile name, config file path, or credentials file path is provided, use
 * {@link ProfileCredentialsProvider}</li>
 * <li>If not, check all places mentioned in {@link DefaultCredentialsProvider} and fall back to
 * {@link AnonymousCredentialsProvider}</li>
 * </ol>
 *
 * @see ProfileCredentialsProvider
 * @see DefaultCredentialsProvider
 * @see AnonymousCredentialsProvider
 */
enum ResolvingCredentials implements AwsSdkV2Credentials {
    INSTANCE;

    private static final AwsCredentialsProviderChain PROVIDER_CHAIN = AwsCredentialsProviderChain.builder()
            .credentialsProviders(
                    DefaultCredentialsProvider.create(),
                    AnonymousCredentialsProvider.create())
            .reuseLastProviderEnabled(false) // Don't cache because this chain is a shared static instance
            .build();

    @Override
    public final AwsCredentialsProvider awsV2CredentialsProvider(@NotNull final S3Instructions instructions) {
        if (instructions.profileName().isPresent()
                || instructions.configFilePath().isPresent()
                || instructions.credentialsFilePath().isPresent()) {
            return ProfileCredentials.INSTANCE.awsV2CredentialsProvider(instructions);
        }
        return PROVIDER_CHAIN;
    }
}
