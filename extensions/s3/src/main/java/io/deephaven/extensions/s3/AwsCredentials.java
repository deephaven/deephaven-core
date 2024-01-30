/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

public interface AwsCredentials {

    /**
     * AWS credentials that looks for credentials in this order:
     * <ol>
     * <li>Java System Properties - {@code aws.accessKeyId} and {@code aws.secretAccessKey}</li>
     * <li>Environment Variables - {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY}</li>
     * <li>Web Identity Token credentials from system properties or environment variables</li>
     * <li>Credential profiles file at the default location (~/.aws/credentials) shared by all AWS SDKs and the AWS
     * CLI</li>
     * <li>Credentials delivered through the Amazon EC2 container service if AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
     * environment variable is set and security manager has permission to access the variable,</li>
     * <li>Instance profile credentials delivered through the Amazon EC2 metadata service</li>
     * </ol>
     */
    static AwsCredentials defaultCredentials() {
        return DefaultCredentials.DEFAULT_CREDENTIALS;
    }

    /**
     * AWS credentials with the specified AWS access key and AWS secret key.
     *
     * @param awsAccessKeyId The AWS access key, used to identify the user interacting with AWS.
     * @param awsSecretAccessKey The AWS secret access key, used to authenticate the user interacting with AWS.
     */
    static AwsCredentials basicCredentials(final String awsAccessKeyId, final String awsSecretAccessKey) {
        return BasicCredentials.of(awsAccessKeyId, awsSecretAccessKey);
    }
}
