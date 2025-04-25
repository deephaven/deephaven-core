//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnInstructionsTest {

    @Test
    void unmapped() {
        final ColumnInstructions ci = ColumnInstructions.unmapped();
        assertThat(ci.isUnmapped()).isTrue();
        assertThat(ci.schemaFieldId()).isEmpty();
        assertThat(ci.schemaFieldName()).isEmpty();
        assertThat(ci.partitionFieldId()).isEmpty();
    }

    @Test
    void schemaFieldId() {
        final ColumnInstructions ci = ColumnInstructions.schemaField(42);
        assertThat(ci.isUnmapped()).isFalse();
        assertThat(ci.schemaFieldId()).hasValue(42);
        assertThat(ci.schemaFieldName()).isEmpty();
        assertThat(ci.partitionFieldId()).isEmpty();
    }

    @Test
    void schemaFieldName() {
        final ColumnInstructions ci = ColumnInstructions.schemaFieldName("Foo");
        assertThat(ci.isUnmapped()).isFalse();
        assertThat(ci.schemaFieldId()).isEmpty();
        assertThat(ci.schemaFieldName()).hasValue("Foo");
        assertThat(ci.partitionFieldId()).isEmpty();
    }

    @Test
    void partitionFieldId() {
        final ColumnInstructions ci = ColumnInstructions.partitionField(2025);
        assertThat(ci.isUnmapped()).isFalse();
        assertThat(ci.schemaFieldId()).isEmpty();
        assertThat(ci.schemaFieldName()).isEmpty();
        assertThat(ci.partitionFieldId()).hasValue(2025);
    }
}
