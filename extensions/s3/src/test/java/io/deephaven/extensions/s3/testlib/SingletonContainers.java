//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3.testlib;

import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.S3Instructions.Builder;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.net.URI;

public final class SingletonContainers {

    // This pattern allows the respective images to be spun up as a container once per-JVM as opposed to once per-class
    // or once per-test.
    // https://java.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
    // https://testcontainers.com/guides/testcontainers-container-lifecycle/#_using_singleton_containers

    public static final class RustFS {
        private static final int S3_PORT = 9000;
        private static final String ACCESS_KEY = "deephaven-test-access-key";
        private static final String SECRET_KEY = "deephaven-test-secret-key";

        // RUSTFS_SERVER_DOMAINS is set so RustFS will accept virtual-host style requests; see virtual-host style
        // implementation comments in S3Instructions.
        private static final GenericContainer<?> RUSTFS =
                new GenericContainer<>(DockerImageName.parse(System.getProperty("testcontainers.rustfs.image")))
                        .withExposedPorts(S3_PORT)
                        .withEnv("RUSTFS_ACCESS_KEY", ACCESS_KEY)
                        .withEnv("RUSTFS_SECRET_KEY", SECRET_KEY)
                        .withEnv("RUSTFS_SERVER_DOMAINS", DockerClientFactory.instance().dockerHostIpAddress())
                        // /health returns 200 before storage is ready; /health/ready waits until S3 requests succeed
                        .waitingFor(Wait.forHttp("/health/ready").forPort(S3_PORT).forStatusCode(200));
        static {
            RUSTFS.start();
        }

        public static void init() {
            // no-op, ensures this class is initialized
        }

        public static Builder s3Instructions(Builder builder) {
            return builder
                    .endpointOverride(URI.create(s3Endpoint()))
                    .regionName(region())
                    .credentials(Credentials.basic(ACCESS_KEY, SECRET_KEY));
        }

        public static S3AsyncClient s3AsyncClient() {
            return S3AsyncClient
                    .builder()
                    .endpointOverride(URI.create(s3Endpoint()))
                    .region(Region.of(region()))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                    .build();
        }

        public static String s3Endpoint() {
            return String.format("http://%s:%d", RUSTFS.getHost(), RUSTFS.getMappedPort(S3_PORT));
        }

        public static String region() {
            return Region.US_EAST_1.id();
        }

        public static String accessKey() {
            return ACCESS_KEY;
        }

        public static String secretAccessKey() {
            return SECRET_KEY;
        }
    }
}
