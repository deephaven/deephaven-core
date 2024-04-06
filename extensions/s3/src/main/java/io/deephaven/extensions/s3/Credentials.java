//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

public interface Credentials {

    /**
     * The default credentials.
     *
     * @see <a href="https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html">Default
     *      credentials provider chain</a>
     */
    static Credentials defaultCredentials() {
        return DefaultCredentials.DEFAULT_CREDENTIALS;
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
}
