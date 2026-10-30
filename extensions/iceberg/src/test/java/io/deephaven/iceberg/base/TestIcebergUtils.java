//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.base;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import static io.deephaven.iceberg.base.IcebergUtils.deephavenNullOrderMatchesIceberg;
import static io.deephaven.iceberg.base.IcebergUtils.deephavenOrderMatchesIceberg;
import static io.deephaven.iceberg.base.IcebergUtils.verifyPartitioningColumns;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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

    @Test
    void testDeephavenNullOrderMatchesIceberg() {
        assertThat(deephavenNullOrderMatchesIceberg(SortDirection.ASC, NullOrder.NULLS_FIRST)).isTrue();
        assertThat(deephavenNullOrderMatchesIceberg(SortDirection.DESC, NullOrder.NULLS_LAST)).isTrue();
        assertThat(deephavenNullOrderMatchesIceberg(SortDirection.ASC, NullOrder.NULLS_LAST)).isFalse();
        assertThat(deephavenNullOrderMatchesIceberg(SortDirection.DESC, NullOrder.NULLS_FIRST)).isFalse();
    }

    /**
     * Deephaven orders booleans, integers, dates, times and timestamps as the Iceberg specification does, but not
     * strings or floating point values, nor any other type, nor a column whose type does not belong with the field's.
     */
    @Test
    void testDeephavenOrderMatchesIceberg() {
        assertOrderMatches(Boolean.class, Types.BooleanType.get(), true);
        assertOrderMatches(byte.class, Types.IntegerType.get(), true);
        assertOrderMatches(short.class, Types.IntegerType.get(), true);
        assertOrderMatches(int.class, Types.IntegerType.get(), true);
        assertOrderMatches(long.class, Types.LongType.get(), true);
        assertOrderMatches(LocalDate.class, Types.DateType.get(), true);
        assertOrderMatches(LocalTime.class, Types.TimeType.get(), true);
        assertOrderMatches(Instant.class, Types.TimestampType.withZone(), true);
        assertOrderMatches(LocalDateTime.class, Types.TimestampType.withoutZone(), true);
        assertOrderMatches(Instant.class, Types.TimestampNanoType.withZone(), true);
        assertOrderMatches(LocalDateTime.class, Types.TimestampNanoType.withoutZone(), true);

        assertOrderMatches(String.class, Types.StringType.get(), false);
        assertOrderMatches(float.class, Types.FloatType.get(), false);
        assertOrderMatches(double.class, Types.DoubleType.get(), false);

        assertOrderMatches(BigDecimal.class, Types.DecimalType.of(10, 2), false);
        assertOrderMatches(UUID.class, Types.UUIDType.get(), false);
        assertOrderMatches(byte[].class, Types.FixedType.ofLength(16), false);
        assertOrderMatches(byte[].class, Types.BinaryType.get(), false);
        assertOrderMatches(Object.class, Types.GeometryType.crs84(), false);
        assertOrderMatches(Object.class, Types.GeographyType.crs84(), false);
        assertOrderMatches(Object.class, Types.VariantType.get(), false);
        assertOrderMatches(Object.class, Types.UnknownType.get(), false);
        assertOrderMatches(Object.class,
                Types.StructType.of(Types.NestedField.required(1, "f", Types.IntegerType.get())), false);
        assertOrderMatches(int[].class, Types.ListType.ofRequired(2, Types.IntegerType.get()), false);
        assertOrderMatches(Object.class,
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.IntegerType.get()), false);

        assertOrderMatches(String.class, Types.IntegerType.get(), false);
        assertOrderMatches(double.class, Types.LongType.get(), false);
        assertOrderMatches(int.class, Types.BooleanType.get(), false);
        assertOrderMatches(Instant.class, Types.DateType.get(), false);
    }

    /**
     * Every Iceberg type ID gets an answer, so that a type ID an Iceberg upgrade adds fails here rather than at a
     * write.
     */
    @Test
    void testDeephavenOrderMatchesIcebergAnswersEveryTypeId() {
        for (final Type.TypeID typeId : Type.TypeID.values()) {
            final Type icebergType = () -> typeId;
            assertThatCode(() -> deephavenOrderMatchesIceberg(
                    SortDirection.ASC, NullOrder.NULLS_FIRST, Object.class, icebergType))
                    .as("%s", typeId)
                    .doesNotThrowAnyException();
        }
    }

    /**
     * Assert whether {@code dataType} orders as {@code icebergType} does in both of the null orders Deephaven sorts in,
     * and that neither of the other null orders matches, whatever the types.
     */
    private static void assertOrderMatches(final Class<?> dataType, final Type icebergType, final boolean expected) {
        assertThat(deephavenOrderMatchesIceberg(SortDirection.ASC, NullOrder.NULLS_FIRST, dataType, icebergType))
                .as("%s as %s ascending", dataType, icebergType)
                .isEqualTo(expected);
        assertThat(deephavenOrderMatchesIceberg(SortDirection.DESC, NullOrder.NULLS_LAST, dataType, icebergType))
                .as("%s as %s descending", dataType, icebergType)
                .isEqualTo(expected);
        assertThat(deephavenOrderMatchesIceberg(SortDirection.ASC, NullOrder.NULLS_LAST, dataType, icebergType))
                .as("%s as %s ascending, nulls last", dataType, icebergType)
                .isFalse();
        assertThat(deephavenOrderMatchesIceberg(SortDirection.DESC, NullOrder.NULLS_FIRST, dataType, icebergType))
                .as("%s as %s descending, nulls first", dataType, icebergType)
                .isFalse();
    }
}
