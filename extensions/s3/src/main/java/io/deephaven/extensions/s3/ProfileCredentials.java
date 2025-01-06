//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

/**
 * Profile specific credentials that uses a profile file.
 *
 * @see ProfileCredentialsProvider
 */
enum ProfileCredentials implements AwsSdkV2Credentials {
    INSTANCE;

    @Override
    public final AwsCredentialsProvider awsV2CredentialsProvider(@NotNull final S3Instructions instructions) {
        final ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider.builder();
        instructions.profileName().ifPresent(builder::profileName);
        instructions.aggregatedProfileFile().ifPresent(builder::profileFile);
        return builder.build();
    }
}
