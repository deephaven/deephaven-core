/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

public interface AwsCredentials {

    static AwsCredentials defaultCredentials() {
        return DefaultCredentials.DEFAULT_CREDENTIALS;
    }

    static AwsCredentials staticCredentials(final String awsAccessKeyId, final String awsSecretAccessKey) {
        return StaticCredentials.of(awsAccessKeyId, awsSecretAccessKey);
    }
}
