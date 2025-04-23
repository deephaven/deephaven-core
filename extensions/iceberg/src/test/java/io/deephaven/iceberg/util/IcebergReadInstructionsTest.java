//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
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

    @Test
    void usePartitionInference() {
        assertThat(IcebergReadInstructions.DEFAULT)
                .extracting(IcebergReadInstructions::usePartitionInference)
                .isEqualTo(false);
        assertThat(IcebergReadInstructions.builder().usePartitionInference(true).build())
                .extracting(IcebergReadInstructions::usePartitionInference)
                .isEqualTo(true);
        try {
            IcebergReadInstructions.builder()
                    .resolver(Resolver.builder()
                            .schema(new Schema())
                            .definition(TableDefinition.of(List.of()))
                            .build())
                    .usePartitionInference(true)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("usePartitionInference should not be true when a resolver is present");
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
