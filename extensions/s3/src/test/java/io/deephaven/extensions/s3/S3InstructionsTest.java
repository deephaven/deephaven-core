//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import java.time.Duration;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class S3InstructionsTest {

    @Test
    public void defaults() {
        final S3Instructions instructions = S3Instructions.builder().regionName("some-region").build();
        assertThat(instructions.regionName()).isEqualTo("some-region");
        assertThat(instructions.maxConcurrentRequests()).isEqualTo(50);
        assertThat(instructions.readAheadCount()).isEqualTo(1);
        assertThat(instructions.fragmentSize()).isEqualTo(5 * (1 << 20));
        assertThat(instructions.maxCacheSize()).isEqualTo(32);
        assertThat(instructions.connectionTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(instructions.readTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(instructions.credentials()).isEqualTo(Credentials.defaultCredentials());
        assertThat(instructions.endpointOverride()).isEmpty();
    }

    @Test
    public void missingRegion() {
        try {
            S3Instructions.builder().build();
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("regionName");
        }
    }

    @Test
    public void minMaxConcurrentRequests() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .maxConcurrentRequests(1)
                .build()
                .maxConcurrentRequests())
                .isEqualTo(1);
    }

    @Test
    public void tooSmallMaxConcurrentRequests() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .maxConcurrentRequests(0)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("maxConcurrentRequests");
        }
    }

    @Test
    public void minReadAheadCount() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .readAheadCount(0)
                .build()
                .readAheadCount())
                .isZero();
    }

    @Test
    public void tooSmallReadAheadCount() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .readAheadCount(-1)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("readAheadCount");
        }
    }

    @Test
    public void minFragmentSize() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .fragmentSize(8 * (1 << 10))
                .build()
                .fragmentSize())
                .isEqualTo(8 * (1 << 10));
    }

    @Test
    public void tooSmallFragmentSize() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .fragmentSize(8 * (1 << 10) - 1)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("fragmentSize");
        }
    }

    @Test
    public void maxFragmentSize() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .fragmentSize(S3Instructions.MAX_FRAGMENT_SIZE)
                .build()
                .fragmentSize())
                .isEqualTo(S3Instructions.MAX_FRAGMENT_SIZE);
    }

    @Test
    public void tooBigFragmentSize() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .fragmentSize(S3Instructions.MAX_FRAGMENT_SIZE + 1)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("fragmentSize");
        }
    }

    @Test
    public void minMaxCacheSize() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .readAheadCount(99)
                .maxCacheSize(100)
                .build()
                .maxCacheSize())
                .isEqualTo(100);
    }

    @Test
    public void tooSmallCacheSize() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .readAheadCount(99)
                    .maxCacheSize(99)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("maxCacheSize");
        }
    }

    @Test
    public void basicCredentials() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .credentials(Credentials.basic("foo", "bar"))
                .build()
                .credentials())
                .isEqualTo(Credentials.basic("foo", "bar"));
    }

    @Test
    public void badCredentials() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .credentials(new Credentials() {})
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("credentials");
        }
    }
}
