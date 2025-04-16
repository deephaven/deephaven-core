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
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorDirect;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorDirect;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
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
 * generate-pyiceberg-3.py for more details.
 */
@Tag("security-manager-allow")
class PyIceberg3Test {
    private static final Namespace NAMESPACE = Namespace.of("list_test");
    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "data");

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.of("bin_col",
                    Type.byteType().arrayType()),
            ColumnDefinition.of("fixed_col",
                    Type.byteType().arrayType()),
            ColumnDefinition.of("long_list",
                    LongVector.type()),
            ColumnDefinition.of("bool_list",
                    ObjectVector.type(Type.booleanType().boxedType())),
            ColumnDefinition.of("double_list",
                    DoubleVector.type()),
            ColumnDefinition.of("float_list",
                    FloatVector.type()),
            ColumnDefinition.of("int_list",
                    IntVector.type()),
            ColumnDefinition.of("string_list",
                    ObjectVector.type(Type.stringType())),
            ColumnDefinition.of("timestamp_ntz_list",
                    ObjectVector.type((GenericType<?>) Type.find(LocalDateTime.class))),
            ColumnDefinition.of("timestamp_tz_list",
                    ObjectVector.type((GenericType<?>) Type.find(Instant.class))),
            ColumnDefinition.of("date_list",
                    ObjectVector.type((GenericType<?>) Type.find(LocalDate.class))),
            ColumnDefinition.of("time_list",
                    ObjectVector.type((GenericType<?>) Type.find(LocalTime.class))),
            ColumnDefinition.of("decimal_list",
                    ObjectVector.type((GenericType<?>) Type.find(BigDecimal.class))));

    private static final byte[] EMPTY_BYTES = new byte[0];

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
        assertThat(fromIceberg.size()).isEqualTo(3);
        final Table expectedData = TableTools.newTable(
                TABLE_DEFINITION,
                TableTools.col("bin_col",
                        "variable length data".getBytes(StandardCharsets.UTF_8),
                        EMPTY_BYTES,
                        null),
                TableTools.col("fixed_col",
                        "123456789ABCD".getBytes(StandardCharsets.UTF_8),
                        "13 bytes only".getBytes(StandardCharsets.UTF_8),
                        null),
                TableTools.col("long_list",
                        new LongVectorDirect(100L, 200L, 300L),
                        new LongVectorDirect(600L, NULL_LONG, 700L),
                        (LongVector) null),
                TableTools.col("bool_list",
                        new ObjectVectorDirect<>(true, false),
                        new ObjectVectorDirect<>(true, false, null),
                        (ObjectVector<Boolean>) null),
                TableTools.col("double_list",
                        new DoubleVectorDirect(10.01, 20.02),
                        new DoubleVectorDirect(60.06, NULL_DOUBLE, 70.07),
                        (DoubleVector) null),
                TableTools.col("float_list",
                        new FloatVectorDirect(1.1f, 2.2f),
                        new FloatVectorDirect(NULL_FLOAT, 5.5f, 6.6f),
                        (FloatVector) null),
                TableTools.col("int_list",
                        new IntVectorDirect(10, 20),
                        new IntVectorDirect(50, NULL_INT, 60),
                        (IntVector) null),
                TableTools.col("string_list",
                        new ObjectVectorDirect<>("hello", "world"),
                        new ObjectVectorDirect<>(null, "alpha", "beta"),
                        (ObjectVector<String>) null),
                TableTools.col("timestamp_ntz_list",
                        new ObjectVectorDirect<>(
                                LocalDateTime.of(2025, 1, 1, 12, 0, 1),
                                LocalDateTime.of(2025, 1, 1, 12, 0, 2)),
                        new ObjectVectorDirect<>(
                                LocalDateTime.of(2025, 1, 3, 14, 0, 1),
                                null),
                        (ObjectVector<LocalDateTime>) null),
                TableTools.col("timestamp_tz_list",
                        new ObjectVectorDirect<>(
                                Instant.parse("2025-01-01T12:00:03Z"),
                                Instant.parse("2025-01-01T12:00:04Z")),
                        new ObjectVectorDirect<>(
                                null,
                                Instant.parse("2025-01-03T14:00:04Z")),
                        (ObjectVector<Instant>) null),
                TableTools.col("date_list",
                        new ObjectVectorDirect<>(
                                LocalDate.of(2025, 1, 1),
                                LocalDate.of(2025, 1, 2)),
                        new ObjectVectorDirect<>(
                                LocalDate.of(2025, 1, 5),
                                null,
                                LocalDate.of(2025, 1, 6),
                                null),
                        (ObjectVector<LocalDate>) null),
                TableTools.col("time_list",
                        new ObjectVectorDirect<>(
                                LocalTime.of(12, 0, 1),
                                LocalTime.of(13, 0, 2)),
                        new ObjectVectorDirect<>(
                                null,
                                LocalTime.of(17, 0, 6),
                                null),
                        (ObjectVector<LocalTime>) null),
                TableTools.col("decimal_list",
                        new ObjectVectorDirect<>(
                                new BigDecimal("123.45"),
                                new BigDecimal("678.90")),
                        new ObjectVectorDirect<>(
                                null,
                                null,
                                (BigDecimal) null),
                        (ObjectVector<BigDecimal>) null));
        TstUtils.assertTableEquals(expectedData, fromIceberg);
    }
}
