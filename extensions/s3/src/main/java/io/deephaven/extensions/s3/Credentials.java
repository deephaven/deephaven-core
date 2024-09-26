//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

public interface Credentials {

    /**
     * Default AWS credentials provider that looks for credentials in the following order:
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
    static Credentials defaultCredentials() {
        return DeephavenDefaultCredentials.INSTANCE;
    }

    /**
     * Default AWS credentials provider that looks for credentials at a number of locations as described in
     * {@link DefaultCredentialsProvider}
     *
     * @see DefaultCredentialsProvider
     */
    static Credentials awsDefaultCredentials() {
        return AWSDefaultCredentials.INSTANCE;
    }

    /**
     * Basic credentials with the specified access key id and secret access key.
     *
     * @param accessKeyId the access key id, used to identify the user
     * @param secretAccessKey the secret access key, used to authenticate the user
     */
    static Credentials basic(final String accessKeyId, final String secretAccessKey) {
        return BasicCredentials.of(accessKeyId, secretAccessKey);
    }

    /**
     * Anonymous credentials. This is useful when the S3 policy has been set to allow anonymous access.
     */
    static Credentials anonymous() {
        return AnonymousCredentials.ANONYMOUS_CREDENTIALS;
    }

    /**
     * Profile specific credentials that uses a custom profile name, configuration file or credentials file.
     *
     * @see ProfileCredentialsProvider
     */
    static Credentials profile() {
        return ProfileCredentials.INSTANCE;
    }
}
