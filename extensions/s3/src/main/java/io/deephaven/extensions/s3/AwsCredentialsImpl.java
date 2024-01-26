/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

interface AwsCredentialsImpl extends AwsCredentials {

    AwsCredentialsProvider awsCredentialsProvider();
}
