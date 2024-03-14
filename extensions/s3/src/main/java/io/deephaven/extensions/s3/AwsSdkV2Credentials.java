//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

interface AwsSdkV2Credentials extends Credentials {

    AwsCredentialsProvider awsV2CredentialsProvider();
}
