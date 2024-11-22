//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.assertj.core.api.Assertions.assertThat;

class IcebergWriteInstructionsTest {

    @Test
    void testSetDhTables() {
        final Table table1 = TableTools.emptyTable(3);
        final Table table2 = TableTools.emptyTable(4);
        final IcebergWriteInstructions instructions = IcebergWriteInstructions.builder()
                .addTables(table1)
                .addTables(table2)
                .build();
        assertThat(instructions.tables()).hasSize(2);
        assertThat(instructions.tables()).contains(table1);
        assertThat(instructions.tables()).contains(table2);
    }

    @Test
    void testSetPartitionPaths() {
        final Table table1 = TableTools.emptyTable(3);
        final String pp1 = "P1C=1/PC2=2";
        final Table table2 = TableTools.emptyTable(4);
        final String pp2 = "P1C=2/PC2=3";
        try {
            final IcebergWriteInstructions instructions = IcebergWriteInstructions.builder()
                    .addPartitionPaths(pp1, pp2)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (final IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("Partition path must be provided for each table");
        }

        final IcebergWriteInstructions instructions = IcebergWriteInstructions.builder()
                .addTables(table1, table2)
                .addPartitionPaths(pp1, pp2)
                .build();
        assertThat(instructions.partitionPaths()).hasSize(2);
        assertThat(instructions.partitionPaths()).contains(pp1);
        assertThat(instructions.partitionPaths()).contains(pp2);
    }
}
