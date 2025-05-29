//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.net.URISyntaxException;
import java.time.LocalTime;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test shows that Deephaven can read binary and list types written by pyiceberg. See TESTING.md and
 * generate-pyiceberg-list-columns.py for more details.
 */
@Tag("security-manager-allow")
class PyIcebergListColumnTest {
    private static final Namespace NAMESPACE = Namespace.of("list_test");
    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "data");

    private static final TableDefinition INFERRED_TD = TableDefinition.of(
            ColumnDefinition.of("bin_col",
                    Type.byteType().arrayType()),
            ColumnDefinition.of("fixed_col",
                    Type.byteType().arrayType()),
            ColumnDefinition.of("long_list",
                    Type.longType().arrayType()),
            ColumnDefinition.of("bool_list",
                    Type.booleanType().boxedType().arrayType()),
            ColumnDefinition.of("double_list",
                    Type.doubleType().arrayType()),
            ColumnDefinition.of("float_list",
                    Type.floatType().arrayType()),
            ColumnDefinition.of("int_list",
                    Type.intType().arrayType()),
            ColumnDefinition.of("string_list",
                    Type.stringType().arrayType()),
            ColumnDefinition.of("timestamp_ntz_list",
                    Type.find(LocalDateTime.class).arrayType()),
            ColumnDefinition.of("timestamp_tz_list",
                    Type.instantType().arrayType()),
            ColumnDefinition.of("date_list",
                    Type.find(LocalDate.class).arrayType()),
            ColumnDefinition.of("time_list",
                    Type.find(LocalTime.class).arrayType()),
            ColumnDefinition.of("decimal_list",
                    Type.find(BigDecimal.class).arrayType()));

    private static final byte[] EMPTY_BYTES = new byte[0];

    private IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp() throws URISyntaxException {
        catalogAdapter = DbResource.openCatalog("list-columns");
    }

    @Test
    void testDefinition() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_IDENTIFIER);
        final TableDefinition td = tableAdapter.definition();
        assertThat(td).isEqualTo(INFERRED_TD);
    }

    @Test
    void testData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_IDENTIFIER);
        final Table fromIceberg = tableAdapter.table();
        assertThat(fromIceberg.size()).isEqualTo(3);
        final Table expectedData = TableTools.newTable(
                INFERRED_TD,
                TableTools.col("bin_col",
                        "variable length data".getBytes(StandardCharsets.UTF_8),
                        EMPTY_BYTES,
                        null),
                TableTools.col("fixed_col",
                        "123456789ABCD".getBytes(StandardCharsets.UTF_8),
                        "13 bytes only".getBytes(StandardCharsets.UTF_8),
                        null),
                TableTools.col("long_list",
                        new long[] {100L, 200L, 300L},
                        new long[] {600L, NULL_LONG, 700L},
                        null),
                TableTools.col("bool_list",
                        new Boolean[] {true, false},
                        new Boolean[] {true, false, null},
                        null),
                TableTools.col("double_list",
                        new double[] {10.01, 20.02},
                        new double[] {60.06, NULL_DOUBLE, 70.07},
                        null),
                TableTools.col("float_list",
                        new float[] {1.1f, 2.2f},
                        new float[] {NULL_FLOAT, 5.5f, 6.6f},
                        null),
                TableTools.col("int_list",
                        new int[] {10, 20},
                        new int[] {50, NULL_INT, 60},
                        null),
                TableTools.col("string_list",
                        new String[] {"hello", "world"},
                        new String[] {null, "alpha", "beta"},
                        null),
                TableTools.col("timestamp_ntz_list",
                        new LocalDateTime[] {
                                LocalDateTime.of(2025, 1, 1, 12, 0, 1),
                                LocalDateTime.of(2025, 1, 1, 12, 0, 2)},
                        new LocalDateTime[] {
                                LocalDateTime.of(2025, 1, 3, 14, 0, 1),
                                null},
                        null),
                TableTools.col("timestamp_tz_list",
                        new Instant[] {
                                Instant.parse("2025-01-01T12:00:03Z"),
                                Instant.parse("2025-01-01T12:00:04Z")},
                        new Instant[] {
                                null,
                                Instant.parse("2025-01-03T14:00:04Z")},
                        null),
                TableTools.col("date_list",
                        new LocalDate[] {
                                LocalDate.of(2025, 1, 1),
                                LocalDate.of(2025, 1, 2)},
                        new LocalDate[] {
                                LocalDate.of(2025, 1, 5),
                                null,
                                LocalDate.of(2025, 1, 6),
                                null},
                        null),
                TableTools.col("time_list",
                        new LocalTime[] {
                                LocalTime.of(12, 0, 1),
                                LocalTime.of(13, 0, 2)},
                        new LocalTime[] {
                                null,
                                LocalTime.of(17, 0, 6),
                                null},
                        null),
                TableTools.col("decimal_list",
                        new BigDecimal[] {
                                new BigDecimal("123.45"),
                                new BigDecimal("678.90")},
                        new BigDecimal[] {null, null, null},
                        null));

        TstUtils.assertTableEquals(expectedData, fromIceberg);
    }
}
