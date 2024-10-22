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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An {@link AwsClientFactory} and {@link S3FileIOAwsClientFactory} implementation that assumes ownership of AWS client
 * creation as configured via {@link S3Instructions}.
 */
public final class DeephavenAwsClientFactory implements AwsClientFactory, S3FileIOAwsClientFactory {

    private static final String UUID_KEY = DeephavenAwsClientFactory.class.getName() + ".__uuid";

    /**
     * Adds {@link DeephavenAwsClientFactory} to {@code propertiesOut} with the keys
     * {@value AwsProperties#CLIENT_FACTORY} and {@value S3FileIOProperties#CLIENT_FACTORY}; it is an error if either of
     * these properties is already set. After the corresponding {@link org.apache.iceberg.catalog.Catalog} is no longer
     * in use, the caller should invoke the returned {@link Runnable} to clean up.
     *
     * @param instructions the instructions
     * @param propertiesOut the properties
     * @return the runnable to be invoked after initialization
     */
    public static Runnable addToProperties(S3Instructions instructions, Map<String, String> propertiesOut) {
        Objects.requireNonNull(instructions);
        putOrThrow(propertiesOut, AwsProperties.CLIENT_FACTORY, DeephavenAwsClientFactory.class.getName());
        putOrThrow(propertiesOut, S3FileIOProperties.CLIENT_FACTORY, DeephavenAwsClientFactory.class.getName());
        final String uuid = UUID.randomUUID().toString();
        putOrThrow(propertiesOut, UUID_KEY, uuid);
        S3_INSTRUCTIONS_MAP.put(uuid, instructions);
        return () -> S3_INSTRUCTIONS_MAP.remove(uuid);
    }

    /**
     * Get the {@link S3Instructions} as set in the corresponding {@link #addToProperties(S3Instructions, Map)} if the
     * properties were built with that. If the properties were built with {@link #addToProperties(S3Instructions, Map)},
     * but the {@link Runnable} was already invoked for cleanup, an {@link IllegalStateException} will be thrown.
     *
     * @param properties the properties
     * @return the instructions
     */
    public static Optional<S3Instructions> getInstructions(Map<String, String> properties) {
        final String uuid = properties.get(UUID_KEY);
        if (uuid == null) {
            return Optional.empty();
        }
        final S3Instructions instructions = S3_INSTRUCTIONS_MAP.get(uuid);
        if (instructions == null) {
            throw new IllegalStateException(
                    "This S3Instructions were already cleaned up; please ensure that the returned Runnable from addToProperties is not invoked until the Catalog is no longer in use.");
        }
        return Optional.of(instructions);
    }

    private static <K, V> void putOrThrow(Map<K, V> map, K key, V value) {
        if (map.putIfAbsent(key, value) != null) {
            throw new IllegalArgumentException(String.format("Key '%s' already exists in map", key));
        }
    }

    private static final Map<String, S3Instructions> S3_INSTRUCTIONS_MAP = new ConcurrentHashMap<>();

    private S3Instructions instructions;

    public DeephavenAwsClientFactory() {
        // This follows the pattern established by other Iceberg classes that have an initialize method; they have a
        // default value that is set in construction, with the expectation that they are properly constructed in the
        // initialize call. While those implementations likely could be stricter and implemented defensively (throwing
        // an exception if any other methods are called before initialize), that does not seem to be the pattern in use.
        // We do not _expect_ the default instructions as set here to ever be used, but we are choosing to follow the
        // "established convention" in the rare case there is some caller misusing this in a way that does not effect
        // the correctness of the end Catalog.
        this.instructions = S3Instructions.DEFAULT;
    }

    @Override
    public void initialize(Map<String, String> properties) {
        this.instructions = getInstructions(properties).orElseThrow(() -> new IllegalArgumentException(
                "DeephavenAwsClientFactory was setup improperly; it must be configured with DeephavenAwsClientFactory.addToProperties"));
    }

    @Override
    public S3Client s3() {
        // Iceberg calls this from org.apache.iceberg.aws.s3.S3FileIO which multiple Catalog implementations use. This
        // implementation is backed by the same configuration primitives that our own async S3 client uses. It is well
        // tested and provides parity between how Iceberg S3 and Deephaven S3 clients are initialized.
        return S3ClientFactory.getSyncClient(instructions);
    }

    @Override
    public GlueClient glue() {
        // Iceberg calls this from org.apache.iceberg.aws.glue.GlueCatalog, and it's possible that other
        // custom Catalog implementations could make use out of this interface. This implementation has been manually
        // tested and confirmed to work in simple cases.
        return GlueClient.builder()
                .applyMutation(b -> S3ClientFactory.applyAllSharedSync(b, instructions))
                .build();
    }

    @Override
    public KmsClient kms() {
        // Iceberg does not call this method. It is likely part of the interface to support advanced authorization that
        // enterprise users may need. It's likely in those scenarios that the user is owning the full Catalog creation
        // as well, with their own custom AwsClientFactory, so it's not clear if this implementation will be of value.
        // That said, it is easy to build and follows the same pattern as the other clients, so it is provided in a
        // "best-effort" basis without further testing.
        return KmsClient.builder()
                .applyMutation(b -> S3ClientFactory.applyAllSharedSync(b, instructions))
                .build();
    }

    @Override
    public DynamoDbClient dynamo() {
        // Iceberg calls this from org.apache.iceberg.aws.dynamodb.DynamoDbCatalog, and it's possible that other
        // custom Catalog implementations could make use out of this interface. This implementation is easy to build
        // and follows the same pattern as the other clients, so it is provided in a "best-effort" basis without further
        // testing.
        return DynamoDbClient.builder()
                .applyMutation(b -> S3ClientFactory.applyAllSharedSync(b, instructions))
                .build();
    }
}
