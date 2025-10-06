//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Deephaven’s default AwsClientFactory implementation. This class internally delegates to Iceberg's default AWS client
 * factory using {@link AwsClientFactories#from} and serves as the central place for DH-specific injection-fixes. For
 * example, we use this to configure additional {@link AsyncHttpClientProperties} for the async s3 client,
 */
public class DeephavenAwsClientFactory implements AwsClientFactory {

    private AwsClientFactory delegate;

    private AwsClientProperties awsClientProperties;
    private S3FileIOProperties s3FileIOProperties;
    private AsyncHttpClientProperties asyncHttpClientProperties;

    @Override
    public void initialize(Map<String, String> properties) {
        this.awsClientProperties = new AwsClientProperties(properties);
        this.s3FileIOProperties = new S3FileIOProperties(properties);
        this.asyncHttpClientProperties = new AsyncHttpClientProperties(properties);

        // Build a delegate using Iceberg's factory selection logic.
        final Map<String, String> baseProps = new HashMap<>(properties);
        baseProps.remove(AwsProperties.CLIENT_FACTORY);
        this.delegate = AwsClientFactories.from(baseProps);
    }

    /* --- Delegate everything we don't customize --- */

    @Override
    public S3Client s3() {
        return delegate.s3();
    }

    @Override
    public GlueClient glue() {
        return delegate.glue();
    }

    @Override
    public KmsClient kms() {
        return delegate.kms();
    }

    @Override
    public DynamoDbClient dynamo() {
        return delegate.dynamo();
    }

    @Override
    public S3AsyncClient s3Async() {
        if (s3FileIOProperties.isS3CRTEnabled()) {
            return delegate.s3Async();
        }
        // TODO (DH-20202): Upstream the HTTP client configuration changes to Iceberg.
        return S3AsyncClient.builder()
                .applyMutation(asyncHttpClientProperties::applyAsyncHttpClientConfigurations)
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
                .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                .build();
    }
}
