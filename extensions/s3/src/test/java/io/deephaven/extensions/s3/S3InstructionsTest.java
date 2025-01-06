//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.profiles.ProfileFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class S3InstructionsTest {

    @Test
    void defaults() {
        final S3Instructions instructions = S3Instructions.builder().build();
        assertThat(instructions.regionName().isEmpty()).isTrue();
        assertThat(instructions.maxConcurrentRequests()).isEqualTo(256);
        assertThat(instructions.readAheadCount()).isEqualTo(32);
        assertThat(instructions.fragmentSize()).isEqualTo(65536);
        assertThat(instructions.connectionTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(instructions.readTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(instructions.credentials()).isEqualTo(Credentials.resolving());
        assertThat(instructions.writePartSize()).isEqualTo(10485760);
        assertThat(instructions.numConcurrentWriteParts()).isEqualTo(64);
        assertThat(instructions.endpointOverride()).isEmpty();
        assertThat(instructions.profileName()).isEmpty();
        assertThat(instructions.configFilePath()).isEmpty();
        assertThat(instructions.credentialsFilePath()).isEmpty();
        assertThat(instructions.aggregatedProfileFile()).isEmpty();
    }

    @Test
    void testSetRegion() {
        final Optional<String> region = S3Instructions.builder()
                .regionName("some-region")
                .build()
                .regionName();
        assertThat(region.isPresent()).isTrue();
        assertThat(region.get()).isEqualTo("some-region");
    }

    @Test
    void testSetMaxConcurrentRequests() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .maxConcurrentRequests(100)
                .build()
                .maxConcurrentRequests())
                .isEqualTo(100);
    }

    @Test
    void testMinMaxConcurrentRequests() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .maxConcurrentRequests(-1)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("maxConcurrentRequests");
        }
    }

    @Test
    void tooSmallMaxConcurrentRequests() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .maxConcurrentRequests(0)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("maxConcurrentRequests");
        }
    }

    @Test
    void minReadAheadCount() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .readAheadCount(0)
                .build()
                .readAheadCount())
                .isZero();
    }

    @Test
    void tooSmallReadAheadCount() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .readAheadCount(-1)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("readAheadCount");
        }
    }

    @Test
    void minFragmentSize() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .fragmentSize(8 * (1 << 10))
                .build()
                .fragmentSize())
                .isEqualTo(8 * (1 << 10));
    }

    @Test
    void tooSmallFragmentSize() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .fragmentSize(8 * (1 << 10) - 1)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("fragmentSize");
        }
    }

    @Test
    void basicCredentials() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .credentials(Credentials.basic("foo", "bar"))
                .build()
                .credentials())
                .isEqualTo(Credentials.basic("foo", "bar"));
    }

    @Test
    void badCredentials() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .credentials(new Credentials() {})
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("credentials");
        }
    }

    @Test
    void tooSmallWritePartSize() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .writePartSize(1024)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("writePartSize");
        }
    }

    @Test
    void tooSmallNumConcurrentWriteParts() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .numConcurrentWriteParts(0)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("numConcurrentWriteParts");
        }
    }

    @Test
    void tooLargeNumConcurrentWriteParts() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .numConcurrentWriteParts(1001)
                    .maxConcurrentRequests(1000)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("numConcurrentWriteParts");
        }
    }

    @Test
    void testSetProfileName() {
        final Optional<String> profileName = S3Instructions.builder()
                .regionName("some-region")
                .profileName("some-profile")
                .build()
                .profileName();
        assertThat(profileName.isPresent()).isTrue();
        assertThat(profileName.get()).isEqualTo("some-profile");
    }

    @Test
    void testSetConfigFilePath() throws IOException {
        // Create temporary config and credentials file and write some data to them
        final Path tempConfigFile = Files.createTempFile("config", ".tmp");
        final String configData = "[default]\nregion = us-east-1\n\n[profile test-user]\nregion = us-east-2";
        Files.write(tempConfigFile, configData.getBytes());

        final Path tempCredentialsFile = Files.createTempFile("credentials", ".tmp");
        final String credentialsData = "[default]\naws_access_key_id = foo\naws_secret_access_key = bar";
        Files.write(tempCredentialsFile, credentialsData.getBytes());

        try {
            final Optional<ProfileFile> ret = S3Instructions.builder()
                    .configFilePath(tempConfigFile.toString())
                    .credentialsFilePath(tempCredentialsFile.toString())
                    .build()
                    .aggregatedProfileFile();
            assertThat(ret.isPresent()).isTrue();
            final ProfileFile profileFile = ret.get();
            assertThat(profileFile.profiles().size()).isEqualTo(2);
            assertThat(profileFile.profile("default").get().properties().get("region")).isEqualTo("us-east-1");
            assertThat(profileFile.profile("default").get().properties().get("aws_access_key_id")).isEqualTo("foo");
            assertThat(profileFile.profile("default").get().properties().get("aws_secret_access_key")).isEqualTo("bar");
            assertThat(profileFile.profile("test-user").get().properties().get("region")).isEqualTo("us-east-2");
        } finally {
            Files.delete(tempConfigFile);
            Files.delete(tempCredentialsFile);
        }
    }

    @Test
    void testBadConfigFilePath() {
        try {
            S3Instructions.builder()
                    .configFilePath("/some/random/path")
                    .build()
                    .aggregatedProfileFile();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("/some/random/path");
        }
    }

    @Test
    void testBadCredentialsFilePath() {
        try {
            S3Instructions.builder()
                    .credentialsFilePath("/some/random/path")
                    .build()
                    .aggregatedProfileFile();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("/some/random/path");
        }
    }
}
