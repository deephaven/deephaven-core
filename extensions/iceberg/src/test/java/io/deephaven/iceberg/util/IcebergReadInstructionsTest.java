//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
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
    void pruningExpressionDefaultsToAlwaysTrue() {
        assertThat(IcebergReadInstructions.DEFAULT.pruningExpression()).isSameAs(Expressions.alwaysTrue());
        assertThat(IcebergReadInstructions.builder().build().pruningExpression())
                .isSameAs(Expressions.alwaysTrue());
    }

    @Test
    void pruningExpression() {
        final Expression expression = Expressions.equal("Foo", "bar");
        final IcebergReadInstructions instructions = IcebergReadInstructions.builder()
                .pruningExpression(expression)
                .build();
        assertThat(instructions.pruningExpression()).isSameAs(expression);
    }

    @Test
    void withPruningExpression() {
        final Expression expression = Expressions.greaterThan("Foo", 42);
        final IcebergReadInstructions instructions = IcebergReadInstructions.DEFAULT
                .withPruningExpression(expression);
        assertThat(instructions.pruningExpression()).isSameAs(expression);
        // Unrelated attributes must be carried over
        assertThat(instructions.updateMode()).isEqualTo(IcebergReadInstructions.DEFAULT.updateMode());
        assertThat(instructions.ignoreResolvingErrors())
                .isEqualTo(IcebergReadInstructions.DEFAULT.ignoreResolvingErrors());
    }

    /**
     * Iceberg {@link Expression}s do not implement {@code equals}. Pinned deliberately: excluding the attribute from
     * equality instead would let instructions with different pruning expressions compare equal, which is far worse than
     * the redundant inequality it causes.
     */
    @Test
    void pruningExpressionEqualityIsReferenceBased() {
        final Expression expression = Expressions.equal("Foo", "bar");
        assertThat(IcebergReadInstructions.builder().pruningExpression(expression).build())
                .isEqualTo(IcebergReadInstructions.builder().pruningExpression(expression).build());

        // Structurally identical, but distinct instances
        assertThat(IcebergReadInstructions.builder().pruningExpression(Expressions.equal("Foo", "bar")).build())
                .isNotEqualTo(IcebergReadInstructions.builder().pruningExpression(Expressions.equal("Foo", "bar"))
                        .build());

        // The default is a singleton, so defaulted instructions still compare equal
        assertThat(IcebergReadInstructions.builder().build()).isEqualTo(IcebergReadInstructions.DEFAULT);
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
