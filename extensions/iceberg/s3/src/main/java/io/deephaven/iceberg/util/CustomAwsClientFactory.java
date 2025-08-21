//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import java.util.Map;

import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * This class replicates the functionality of the Default AWS client factory from
 * {@link org.apache.iceberg.aws.AwsClientFactories} with the addition of {@link AsyncHttpClientProperties}. These new
 * properties allow for the configuration of the asynchronous HTTP client used by the S3AsyncClient.
 */
// TODO (DH-20202): Upstream this class to Iceberg
public class CustomAwsClientFactory implements AwsClientFactory {
    private AwsProperties awsProperties;
    private AwsClientProperties awsClientProperties;
    private S3FileIOProperties s3FileIOProperties;
    private HttpClientProperties httpClientProperties;
    private AsyncHttpClientProperties asyncHttpClientProperties;

    @Override
    public S3Client s3() {
        return S3Client.builder()
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                .applyMutation(s3FileIOProperties::applyServiceConfigurations)
                .applyMutation(
                        b -> s3FileIOProperties.applyCredentialConfigurations(awsClientProperties, b))
                .applyMutation(s3FileIOProperties::applySignerConfiguration)
                .applyMutation(s3FileIOProperties::applyS3AccessGrantsConfigurations)
                .applyMutation(s3FileIOProperties::applyUserAgentConfigurations)
                .applyMutation(s3FileIOProperties::applyRetryConfigurations)
                .build();
    }

    @Override
    public S3AsyncClient s3Async() {
        if (s3FileIOProperties.isS3CRTEnabled()) {
            return S3AsyncClient.crtBuilder()
                    .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                    .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
                    .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                    .applyMutation(s3FileIOProperties::applyS3CrtConfigurations)
                    .build();
        }
        return S3AsyncClient.builder()
                .applyMutation(asyncHttpClientProperties::applyAsyncHttpClientConfigurations)
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
                .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                .build();
    }

    @Override
    public GlueClient glue() {
        return GlueClient.builder()
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .applyMutation(awsProperties::applyGlueEndpointConfigurations)
                .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
                .build();
    }

    @Override
    public KmsClient kms() {
        return KmsClient.builder()
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
                .applyMutation(awsClientProperties::applyRetryConfigurations)
                .build();
    }

    @Override
    public DynamoDbClient dynamo() {
        return DynamoDbClient.builder()
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
                .applyMutation(awsProperties::applyDynamoDbEndpointConfigurations)
                .build();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        this.awsProperties = new AwsProperties(properties);
        this.awsClientProperties = new AwsClientProperties(properties);
        this.s3FileIOProperties = new S3FileIOProperties(properties);
        this.httpClientProperties = new HttpClientProperties(properties);
        this.asyncHttpClientProperties = new AsyncHttpClientProperties(properties);
    }
}
