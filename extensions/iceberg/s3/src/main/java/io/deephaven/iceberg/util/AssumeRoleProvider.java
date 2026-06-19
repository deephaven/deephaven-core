//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.Map;
import java.util.UUID;

public class AssumeRoleProvider implements AwsCredentialsProvider {
    private AwsProperties awsProperties;
    private HttpClientProperties httpClientProperties;
    private S3FileIOProperties s3FileIOProperties;
    private String roleSessionName;
    private AwsClientProperties awsClientProperties;

    public static AssumeRoleProvider create(Map<String, String> properties) {
        return new AssumeRoleProvider(properties);
    }

    private AssumeRoleProvider(Map<String, String> properties) {
        this.awsProperties = new AwsProperties(properties);
        this.s3FileIOProperties = new S3FileIOProperties(properties);
        this.httpClientProperties = new HttpClientProperties(properties);
        this.awsClientProperties = new AwsClientProperties(properties);
        this.roleSessionName = genSessionName();
        Preconditions.checkNotNull(
                awsProperties.clientAssumeRoleArn(),
                "Cannot initialize AssumeRoleProvider with null role ARN");
        Preconditions.checkNotNull(
                awsProperties.clientAssumeRoleRegion(),
                "Cannot initialize AssumeRoleProvider with null region");
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return createCredentialsProvider().resolveCredentials();
    }

    private String genSessionName() {
        if (awsProperties.clientAssumeRoleSessionName() != null) {
            return awsProperties.clientAssumeRoleSessionName();
        }
        return String.format("iceberg-aws-%s", UUID.randomUUID());
    }

    private <T extends AwsClientBuilder & AwsSyncClientBuilder> T applyAssumeRoleConfigurations(T clientBuilder) {
        clientBuilder.region(Region.of(awsProperties.clientAssumeRoleRegion()));
        return clientBuilder;
    }

    private <T extends StsClientBuilder> void applyCredentialConfigurations(
            AwsClientProperties awsClientProperties, T builder) {
        builder.credentialsProvider(
                s3FileIOProperties.isRemoteSigningEnabled()
                        ? AnonymousCredentialsProvider.create()
                        : awsClientProperties.credentialsProvider(
                                s3FileIOProperties.accessKeyId(),
                                s3FileIOProperties.secretAccessKey(),
                                s3FileIOProperties.sessionToken()));
    }

    private StsClient sts() {
        return StsClient.builder()
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .applyMutation(b -> applyCredentialConfigurations(awsClientProperties, b))
                .applyMutation(this::applyAssumeRoleConfigurations) // Added this line
                .build();
    }

    private StsAssumeRoleCredentialsProvider createCredentialsProvider() {
        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(sts())
                .refreshRequest(createAssumeRoleRequest())
                .build();
    }

    private AssumeRoleRequest createAssumeRoleRequest() {
        return AssumeRoleRequest.builder()
                .roleArn(awsProperties.clientAssumeRoleArn())
                .roleSessionName(roleSessionName)
                .durationSeconds(awsProperties.clientAssumeRoleTimeoutSec())
                .externalId(awsProperties.clientAssumeRoleExternalId())
                .tags(awsProperties.stsClientAssumeRoleTags())
                .build();
    }
}
