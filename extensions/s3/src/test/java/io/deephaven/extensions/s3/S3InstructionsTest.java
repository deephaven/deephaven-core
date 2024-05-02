//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class S3InstructionsTest {

    @Test
    void defaults() {
        final S3Instructions instructions = S3Instructions.builder()
                .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                        .regionName("some-region")
                        .build())
                .build();
        assertThat(instructions.readTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(instructions.readAheadCount()).isEqualTo(1);
        assertThat(instructions.fragmentSize()).isEqualTo(5 * (1 << 20));
        assertThat(instructions.maxCacheSize()).isEqualTo(32);

        final DeephavenS3AsyncClientFactory asyncClientFactory =
                (DeephavenS3AsyncClientFactory) instructions.asyncClientFactory();
        assertThat(asyncClientFactory.regionName()).isEqualTo("some-region");
        assertThat(asyncClientFactory.readTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(asyncClientFactory.maxConcurrentRequests()).isEqualTo(50);
        assertThat(asyncClientFactory.connectionTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(asyncClientFactory.credentials()).isEqualTo(Credentials.defaultCredentials());
        assertThat(asyncClientFactory.endpointOverride()).isEmpty();
    }

    @Test
    void missingRegion() {
        try {
            S3Instructions.builder()
                    .asyncClientFactory(DeephavenS3AsyncClientFactory.builder().build())
                    .build();
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("region");
        }
    }

    @Test
    void missingAsyncClientFactory() {
        try {
            S3Instructions.builder().build();
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("asyncClientFactory");
        }
    }

    @Test
    void basicAsyncClientFactory() {
        final S3Instructions input = S3Instructions.builder()
                .asyncClientFactory(() -> null)
                .build();
        assertThat(input.asyncClientFactory().create()).isNull();
    }

    @Test
    void basicAsyncClientFactoryWithReadTimeout() {
        assertThat(S3Instructions.builder()
                .asyncClientFactory(() -> null)
                .readTimeout(Duration.ofSeconds(6))
                .build()
                .readTimeout())
                .isEqualTo(Duration.ofSeconds(6));
    }

    @Test
    void readTimeout() {
        assertThat(S3Instructions.builder()
                .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                        .regionName("some-region")
                        .readTimeout(Duration.ofSeconds(6))
                        .build())
                .build()
                .readTimeout())
                .isEqualTo(Duration.ofSeconds(6));
    }

    @Test
    void inconsistentReadTimeout() {
        try {
            S3Instructions.builder()
                    .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                            .regionName("some-region")
                            .readTimeout(Duration.ofSeconds(6))
                            .build())
                    .readTimeout(Duration.ofSeconds(7))
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("readTimeout");
        }
    }



    @Test
    void minMaxConcurrentRequests() {
        final S3Instructions input = S3Instructions.builder()
                .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                        .regionName("some-region")
                        .maxConcurrentRequests(1)
                        .build())
                .build();
        assertThat(((DeephavenS3AsyncClientFactory) input.asyncClientFactory()).maxConcurrentRequests())
                .isEqualTo(1);

    }

    @Test
    void tooSmallMaxConcurrentRequests() {
        try {
            S3Instructions.builder()
                    .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                            .regionName("some-region")
                            .maxConcurrentRequests(0)
                            .build())
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("maxConcurrentRequests");
        }
    }

    @Test
    void minReadAheadCount() {
        assertThat(S3Instructions.builder()
                .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                        .regionName("some-region")
                        .build())
                .readAheadCount(0)
                .build()
                .readAheadCount())
                .isZero();
    }

    @Test
    void tooSmallReadAheadCount() {
        try {
            S3Instructions.builder()
                    .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                            .regionName("some-region")
                            .build())
                    .readAheadCount(-1)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("readAheadCount");
        }
    }

    @Test
    void minFragmentSize() {
        assertThat(S3Instructions.builder()
                .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                        .regionName("some-region")
                        .build())
                .fragmentSize(8 * (1 << 10))
                .build()
                .fragmentSize())
                .isEqualTo(8 * (1 << 10));
    }

    @Test
    void tooSmallFragmentSize() {
        try {
            S3Instructions.builder()
                    .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                            .regionName("some-region")
                            .build())
                    .fragmentSize(8 * (1 << 10) - 1)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("fragmentSize");
        }
    }

    @Test
    void maxFragmentSize() {
        assertThat(S3Instructions.builder()
                .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                        .regionName("some-region")
                        .build())
                .fragmentSize(S3Instructions.MAX_FRAGMENT_SIZE)
                .build()
                .fragmentSize())
                .isEqualTo(S3Instructions.MAX_FRAGMENT_SIZE);
    }

    @Test
    void tooBigFragmentSize() {
        try {
            S3Instructions.builder()
                    .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                            .regionName("some-region")
                            .build())
                    .fragmentSize(S3Instructions.MAX_FRAGMENT_SIZE + 1)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("fragmentSize");
        }
    }

    @Test
    void minMaxCacheSize() {
        assertThat(S3Instructions.builder()
                .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                        .regionName("some-region")
                        .build())
                .readAheadCount(99)
                .maxCacheSize(100)
                .build()
                .maxCacheSize())
                .isEqualTo(100);
    }

    @Test
    void tooSmallCacheSize() {
        try {
            S3Instructions.builder()
                    .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                            .regionName("some-region")
                            .build())
                    .readAheadCount(99)
                    .maxCacheSize(99)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("maxCacheSize");
        }
    }

    @Test
    void basicCredentials() {
        final S3Instructions input = S3Instructions.builder()
                .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                        .regionName("some-region")
                        .credentials(Credentials.basic("foo", "bar"))
                        .build())
                .build();
        assertThat(((DeephavenS3AsyncClientFactory) input.asyncClientFactory()).credentials())
                .isEqualTo(Credentials.basic("foo", "bar"));
    }

    @Test
    void badCredentials() {
        try {
            S3Instructions.builder()
                    .asyncClientFactory(DeephavenS3AsyncClientFactory.builder()
                            .regionName("some-region")
                            .credentials(new Credentials() {})
                            .build())
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("credentials");
        }
    }
}
