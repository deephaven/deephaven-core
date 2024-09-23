//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An {@link AwsClientFactory} and {@link S3FileIOAwsClientFactory} implementation that assumes ownership of AWS client
 * creation as configured via {@link S3Instructions}.
 */
public class DeephavenAwsClientFactory implements AwsClientFactory, S3FileIOAwsClientFactory {

    private static final String UUID_KEY = DeephavenAwsClientFactory.class.getName() + ".__uuid";

    /**
     * Adds {@link DeephavenAwsClientFactory} to {@code propertiesOut} with the keys
     * {@value AwsProperties#CLIENT_FACTORY} and {@value S3FileIOProperties#CLIENT_FACTORY}; it is an error if either of
     * these properties is already set. Also sets {@value S3FileIOProperties#PRELOAD_CLIENT_ENABLED} to "true". After
     * the necessary objects have been initialized, the caller should call the returned {@link Runnable} to clean up.
     *
     * @param instructions the instructions
     * @param propertiesOut the properties
     * @return the runnable to be invoked after initialization
     */
    public static Runnable addToProperties(S3Instructions instructions, Map<String, String> propertiesOut) {
        Objects.requireNonNull(instructions);
        if (propertiesOut.putIfAbsent(AwsProperties.CLIENT_FACTORY,
                DeephavenAwsClientFactory.class.getName()) != null) {
            throw new IllegalArgumentException(
                    String.format("Trying to put '%s', but it already exists", AwsProperties.CLIENT_FACTORY));
        }
        if (propertiesOut.putIfAbsent(S3FileIOProperties.CLIENT_FACTORY,
                DeephavenAwsClientFactory.class.getName()) != null) {
            throw new IllegalArgumentException(
                    String.format("Trying to put '%s', but it already exists", S3FileIOProperties.CLIENT_FACTORY));
        }
        final String uuid = UUID.randomUUID().toString();
        if (propertiesOut.putIfAbsent(UUID_KEY, uuid) != null) {
            throw new IllegalArgumentException(
                    String.format("Trying to put '%s', but it already exists", UUID_KEY));
        }
        propertiesOut.put(S3FileIOProperties.PRELOAD_CLIENT_ENABLED, "true");
        // Note: glue client is already preloaded on init if needed
        S3_INSTRUCTIONS_MAP.put(uuid, instructions);
        return () -> S3_INSTRUCTIONS_MAP.remove(uuid);
    }

    private static final Map<String, S3Instructions> S3_INSTRUCTIONS_MAP = new ConcurrentHashMap<>();

    private S3Instructions instructions;

    @Override
    public S3Client s3() {
        return S3ClientFactory.getSyncClient(instructions);
    }

    @Override
    public GlueClient glue() {
        return GlueClient.builder()
                .applyMutation(b -> S3ClientFactory.applySyncHttpClient(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyOverrideConfiguration(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyCredentialsProvider(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyRegion(b, instructions))
                // .applyMutation(b -> AwsClientFactory.applyCrossRegionAccess(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyEndpointOverride(b, instructions))
                .build();
    }

    @Override
    public KmsClient kms() {
        return KmsClient.builder()
                .applyMutation(b -> S3ClientFactory.applySyncHttpClient(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyOverrideConfiguration(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyCredentialsProvider(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyRegion(b, instructions))
                // .applyMutation(b -> AwsClientFactory.applyCrossRegionAccess(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyEndpointOverride(b, instructions))
                .build();
    }

    @Override
    public DynamoDbClient dynamo() {
        return DynamoDbClient.builder()
                .applyMutation(b -> S3ClientFactory.applySyncHttpClient(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyOverrideConfiguration(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyCredentialsProvider(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyRegion(b, instructions))
                // .applyMutation(b -> AwsClientFactory.applyCrossRegionAccess(b, instructions))
                .applyMutation(b -> S3ClientFactory.applyEndpointOverride(b, instructions))
                .build();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        final String uuid = properties.get(UUID_KEY);
        if (uuid == null) {
            throw new IllegalArgumentException(
                    "DeephavenAwsClientFactory was setup improperly; it must be configured with DeephavenAwsClientFactory.addToProperties");
        }
        final S3Instructions instructions = S3_INSTRUCTIONS_MAP.get(uuid);
        if (instructions == null) {
            throw new IllegalStateException("This DeephavenAwsClientFactory was already cleaned up");
        }
        this.instructions = instructions;
    }
}
