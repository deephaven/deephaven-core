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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test shows that we read binary and list types written by pyiceberg. See TESTING.md and generate-pyiceberg-3.py
 * for more details.
 */
@Tag("security-manager-allow")
class PyIceberg3Test {
    private static final Namespace NAMESPACE = Namespace.of("list_test");
    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "data");

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.of("bin_col", Type.find(byte[].class)),
            ColumnDefinition.of("fixed_col", Type.find(byte[].class)),
            ColumnDefinition.of("long_list", Type.find(long[].class)),
            ColumnDefinition.of("bool_list", Type.find(Boolean[].class)),
            ColumnDefinition.of("double_list", Type.find(double[].class)),
            ColumnDefinition.of("float_list", Type.find(float[].class)),
            ColumnDefinition.of("int_list", Type.find(int[].class)),
            ColumnDefinition.of("string_list", Type.find(String[].class)),
            ColumnDefinition.of("timestamp_ntz_list", Type.find(LocalDateTime[].class)),
            ColumnDefinition.of("timestamp_tz_list", Type.find(Instant[].class)),
            ColumnDefinition.of("date_list", Type.find(LocalDate[].class)),
            ColumnDefinition.of("time_list", Type.find(LocalTime[].class)),
            ColumnDefinition.of("decimal_list", Type.find(BigDecimal[].class)));

    private IcebergCatalogAdapter catalogAdapter;

    @BeforeEach
    void setUp() throws URISyntaxException {
        catalogAdapter = DbResource.openCatalog("pyiceberg-3");
    }

    @Test
    void testDefinition() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_IDENTIFIER);
        final TableDefinition td = tableAdapter.definition();
        assertThat(td).isEqualTo(TABLE_DEFINITION);
    }

    @Test
    void testData() {
        final IcebergTableAdapter tableAdapter = catalogAdapter.loadTable(TABLE_IDENTIFIER);
        final Table fromIceberg = tableAdapter.table();
        assertThat(fromIceberg.size()).isEqualTo(2);
        final Table expectedData = TableTools.newTable(
                TABLE_DEFINITION,
                TableTools.col("bin_col",
                        "variable length data".getBytes(StandardCharsets.UTF_8),
                        new byte[0]),
                TableTools.col("fixed_col",
                        "123456789ABCD".getBytes(StandardCharsets.UTF_8),
                        "13 bytes only".getBytes(StandardCharsets.UTF_8)),
                TableTools.col("long_list",
                        new long[] {100L, 200L, 300L},
                        new long[] {400L, 500L}),
                TableTools.col("bool_list",
                        new Boolean[] {true, false},
                        new Boolean[] {false, true}),
                TableTools.col("double_list",
                        new double[] {10.01, 20.02},
                        new double[] {30.03, 40.04}),
                TableTools.col("float_list",
                        new float[] {1.1f, 2.2f},
                        new float[] {3.3f, 4.4f}),
                TableTools.col("int_list",
                        new int[] {10, 20},
                        new int[] {30, 40}),
                TableTools.col("string_list",
                        new String[] {"hello", "world"},
                        new String[] {"foo", "bar", "baz"}),
                TableTools.col("timestamp_ntz_list",
                        new LocalDateTime[] {
                                LocalDateTime.of(2025, 1, 1, 12, 0, 1),
                                LocalDateTime.of(2025, 1, 1, 12, 0, 2)
                        },
                        new LocalDateTime[] {
                                LocalDateTime.of(2025, 1, 2, 13, 0, 1),
                                LocalDateTime.of(2025, 1, 2, 13, 0, 2)
                        }),
                TableTools.col("timestamp_tz_list",
                        new Instant[] {
                                Instant.parse("2025-01-01T12:00:03Z"),
                                Instant.parse("2025-01-01T12:00:04Z")
                        },
                        new Instant[] {
                                Instant.parse("2025-01-02T13:00:03Z"),
                                Instant.parse("2025-01-02T13:00:04Z")
                        }),
                TableTools.col("date_list",
                        new LocalDate[] {
                                LocalDate.of(2025, 1, 1),
                                LocalDate.of(2025, 1, 2)
                        },
                        new LocalDate[] {
                                LocalDate.of(2025, 1, 3),
                                LocalDate.of(2025, 1, 4)
                        }),
                TableTools.col("time_list",
                        new LocalTime[] {
                                LocalTime.of(12, 0, 1),
                                LocalTime.of(13, 0, 2)
                        },
                        new LocalTime[] {
                                LocalTime.of(14, 0, 3),
                                LocalTime.of(15, 0, 4)
                        }),
                TableTools.col("decimal_list",
                        new BigDecimal[] {
                                new BigDecimal("123.45"),
                                new BigDecimal("678.90")
                        },
                        new BigDecimal[] {
                                new BigDecimal("234.56"),
                                new BigDecimal("987.65")
                        }));
        TstUtils.assertTableEquals(expectedData, fromIceberg);
    }
}
