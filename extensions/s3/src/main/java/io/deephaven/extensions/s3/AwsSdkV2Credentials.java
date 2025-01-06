//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.util.annotations.InternalUseOnly;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@InternalUseOnly
public interface AwsSdkV2Credentials extends Credentials {

    /**
     * Get the AWS credentials provider based on the given instructions.
     */
    AwsCredentialsProvider awsV2CredentialsProvider(S3Instructions instructions);
}
