//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static io.deephaven.iceberg.base.IcebergUtils.verifyPartitioningColumns;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class TestIcebergUtils {
    private static final String PART_COL1_NAME = "PartCol1";
    private static final String PART_COL2_NAME = "PartCol2";
    private static final String NONPART_COL1_NAME = "intCol";
    private static final String NONPART_COL2_NAME = "doubleCol";

    private static final String RESOLVED_PART_COL_NAME = "ResolvedCol";

    /**
     * Creates a {@link Schema} and {@link PartitionSpec} with 2 partitioning columns (`PartCol1`, `PartCol2`)
     *
     * @return a {@link PartitionSpec} which includes 2 partitioning columns (`PartCol1`, `PartCol2`)
     */
    private static PartitionSpec getPartitionSpec() {
        final Schema schema = new Schema(
                Types.NestedField.builder().withId(1).withName(PART_COL1_NAME).isOptional(false)
                        .ofType(Types.StringType.get()).build(),
                Types.NestedField.builder().withId(2).withName(PART_COL2_NAME).isOptional(false)
                        .ofType(Types.IntegerType.get()).build(),
                Types.NestedField.builder().withId(3).withName(NONPART_COL1_NAME).isOptional(true)
                        .ofType(Types.IntegerType.get()).build(),
                Types.NestedField.builder().withId(4).withName(NONPART_COL2_NAME).isOptional(true)
                        .ofType(Types.DoubleType.get()).build());

        return PartitionSpec.builderFor(schema)
                .identity(PART_COL1_NAME)
                .identity(PART_COL2_NAME)
                .build();
    }

    private static ColumnDefinition<?> partCol1() {
        return ColumnDefinition.ofString(PART_COL1_NAME).withPartitioning();
    }

    private static ColumnDefinition<?> partCol2() {
        return ColumnDefinition.ofInt(PART_COL2_NAME).withPartitioning();
    }

    private static ColumnDefinition<?> resolvedPartCol() {
        return ColumnDefinition.ofInt(RESOLVED_PART_COL_NAME).withPartitioning();
    }

    private static ColumnDefinition<?> nonPartCol1() {
        return ColumnDefinition.ofInt(NONPART_COL1_NAME);
    }

    private static ColumnDefinition<?> nonPartCol2() {
        return ColumnDefinition.ofDouble(NONPART_COL2_NAME);
    }

    @Test
    void testVerifyPartitioningColumns() {
        final PartitionSpec spec = getPartitionSpec();

        // match partitioning-column ordering
        final TableDefinition tDef1 = TableDefinition.of(partCol1(), partCol2(), nonPartCol1(), nonPartCol2());
        verifyPartitioningColumns(spec, tDef1);

        // flip partitioning-column ordering
        final TableDefinition tDef2 = TableDefinition.of(partCol2(), partCol1(), nonPartCol1(), nonPartCol2());
        verifyPartitioningColumns(spec, tDef2);
    }

    @Test
    void testMissingPartitionColumn() {
        final PartitionSpec spec = getPartitionSpec();

        // missing "PartCol1"
        final TableDefinition tDef1 = TableDefinition.of(partCol2(), nonPartCol1(), nonPartCol2());
        try {
            verifyPartitioningColumns(spec, tDef1);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (final IllegalArgumentException ise) {
            assertThat(ise.getMessage()).startsWith(
                    "Partition spec contains 2 fields, but the table definition contains 1 fields, partition spec");
        }

        // missing "PartCol2"
        final TableDefinition tDef2 = TableDefinition.of(partCol1(), nonPartCol1(), nonPartCol2());
        try {
            verifyPartitioningColumns(spec, tDef2);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (final IllegalArgumentException ise) {
            assertThat(ise.getMessage()).startsWith(
                    "Partition spec contains 2 fields, but the table definition contains 1 fields, partition spec");
        }
    }

    @Test
    void testInvalidPartitioningColumns() {
        // missing "PartCol2", added "ResolvedCol", but not defining the mapping in resolver. count is correct, but we
        // do not match by name
        final TableDefinition tDef1 = TableDefinition.of(partCol1(), resolvedPartCol(), nonPartCol1(), nonPartCol2());
        try {
            verifyPartitioningColumns(getPartitionSpec(), tDef1);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (final IllegalArgumentException ise) {
            assertThat(ise.getMessage())
                    .startsWith("Partitioning column " + PART_COL2_NAME
                            + " is not present in the table definition TableDefinition");
        }
    }
}
