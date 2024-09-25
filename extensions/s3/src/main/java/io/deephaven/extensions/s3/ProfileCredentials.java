//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.annotations.CopyableStyle;
import org.immutables.value.Value;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;

import java.util.Optional;

/**
 * A set of credentials that are sourced from a profile file. This class is not public since it is not intended to be
 * used directly by users. Instead, users should use the {@link S3Instructions} class to provide config and credentials
 * files.
 */
@Value.Immutable
@CopyableStyle
abstract class ProfileCredentials implements AwsSdkV2Credentials {

    static ProfileCredentials.Builder builder() {
        return ImmutableProfileCredentials.builder();
    }

    abstract Optional<ProfileFile> profileFile();

    abstract Optional<String> profileName();

    interface Builder {
        Builder profileFile(ProfileFile profileFile);

        Builder profileName(String profileName);

        ProfileCredentials build();
    }

    @Override
    public final AwsCredentialsProvider awsV2CredentialsProvider() {
        final ProfileCredentialsProvider.Builder providerBuilder = ProfileCredentialsProvider.builder();
        profileFile().ifPresent(providerBuilder::profileFile);
        profileName().ifPresent(providerBuilder::profileName);
        return providerBuilder.build();
    }
}
