//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

class IcebergReadInstructionsTest {
    @Test
    void snapshot() {
        try {
            IcebergReadInstructions.builder()
                    .snapshot(MockSnapshot.MOCK)
                    .snapshotId(42)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e)
                    .hasMessageContaining("If both snapshotID and snapshot are provided, the snapshot Ids must match");
        }
    }

    enum MockSnapshot implements Snapshot {
        MOCK;

        @Override
        public long sequenceNumber() {
            return 0;
        }

        @Override
        public long snapshotId() {
            return 0;
        }

        @Override
        public Long parentId() {
            return 0L;
        }

        @Override
        public long timestampMillis() {
            return 0;
        }

        @Override
        public List<ManifestFile> allManifests(FileIO io) {
            return List.of();
        }

        @Override
        public List<ManifestFile> dataManifests(FileIO io) {
            return List.of();
        }

        @Override
        public List<ManifestFile> deleteManifests(FileIO io) {
            return List.of();
        }

        @Override
        public String operation() {
            return "";
        }

        @Override
        public Map<String, String> summary() {
            return Map.of();
        }

        @Override
        public Iterable<DataFile> addedDataFiles(FileIO io) {
            return null;
        }

        @Override
        public Iterable<DataFile> removedDataFiles(FileIO io) {
            return null;
        }

        @Override
        public String manifestListLocation() {
            return "";
        }
    }
}
