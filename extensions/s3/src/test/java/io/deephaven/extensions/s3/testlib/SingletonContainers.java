//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3.testlib;

import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.S3Instructions.Builder;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
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

    public static final class LocalStack {
        private static final LocalStackContainer LOCALSTACK_S3 =
                new LocalStackContainer(DockerImageName.parse(System.getProperty("testcontainers.localstack.image"))
                        .asCompatibleSubstituteFor("localstack/localstack"), false)
                        .withServices(Service.S3);
        static {
            LOCALSTACK_S3.start();
        }

        public static void init() {
            // no-op, ensures this class is initialized
        }

        public static Builder s3Instructions(Builder builder) {
            return builder
                    .endpointOverride(LOCALSTACK_S3.getEndpoint())
                    .regionName(LOCALSTACK_S3.getRegion())
                    .credentials(Credentials.basic(LOCALSTACK_S3.getAccessKey(), LOCALSTACK_S3.getSecretKey()));
        }

        public static S3AsyncClient s3AsyncClient() {
            return S3AsyncClient
                    .crtBuilder()
                    .endpointOverride(LOCALSTACK_S3.getEndpoint())
                    .region(Region.of(LOCALSTACK_S3.getRegion()))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(LOCALSTACK_S3.getAccessKey(), LOCALSTACK_S3.getSecretKey())))
                    .build();
        }
    }

    public static final class MinIO {
        // MINIO_DOMAIN is set so MinIO will accept virtual-host style requests; see virtual-host style implementation
        // comments in S3Instructions.
        // https://min.io/docs/minio/linux/reference/minio-server/settings/core.html#domain
        private static final MinIOContainer MINIO =
                new MinIOContainer(DockerImageName.parse(System.getProperty("testcontainers.minio.image"))
                        .asCompatibleSubstituteFor("minio/minio"))
                        .withEnv("MINIO_DOMAIN", DockerClientFactory.instance().dockerHostIpAddress());
        static {
            MINIO.start();
        }

        public static void init() {
            // no-op, ensures this class is initialized
        }

        public static Builder s3Instructions(Builder builder) {
            return builder
                    .endpointOverride(URI.create(MINIO.getS3URL()))
                    .regionName(Region.AWS_GLOBAL.id())
                    .credentials(Credentials.basic(MINIO.getUserName(), MINIO.getPassword()));
        }

        public static S3AsyncClient s3AsyncClient() {
            return S3AsyncClient
                    .crtBuilder()
                    .endpointOverride(URI.create(MINIO.getS3URL()))
                    .region(Region.AWS_GLOBAL)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())))
                    .build();
        }
    }
}
