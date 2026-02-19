//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalTime;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static io.deephaven.engine.util.TableTools.col;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive end-to-end test for all supported types in FieldAdapter/FieldVectorAdapter. Tests scalar values, array
 * types, and null handling for each type.
 */
public class SnapshotAllTypesTest extends GrpcTableOperationTestBase<SnapshotTableRequest> {

    @Override
    public ExportedTableCreationResponse send(SnapshotTableRequest request) {
        return channel().tableBlocking().snapshot(request);
    }

    @Test
    public void snapshotAllScalarTypes() {
        // Test all primitive scalar types
        final TableReference allScalarsTable = ref(TableTools.newTable(
                // Integer types
                col("byteCol", (byte) 1, (byte) 2, (byte) 3),
                col("shortCol", (short) 100, (short) 200, (short) 300),
                col("intCol", 1000, 2000, 3000),
                col("longCol", 100000L, 200000L, 300000L),

                // Floating point types
                col("floatCol", 1.5f, 2.5f, 3.5f),
                col("doubleCol", 1.1, 2.2, 3.3),

                // Boolean and character types
                col("booleanCol", true, false, true),
                col("charCol", 'a', 'b', 'c'),

                // String type
                col("stringCol", "hello", "world", "test"),

                // Temporal types
                col("instantCol",
                        Instant.parse("2025-01-01T10:00:00Z"),
                        Instant.parse("2025-01-02T11:00:00Z"),
                        Instant.parse("2025-01-03T12:00:00Z")),
                col("localTimeCol",
                        LocalTime.of(10, 30, 45),
                        LocalTime.of(14, 15, 30),
                        LocalTime.of(22, 45, 0)),
                col("localDateCol",
                        LocalDate.of(2025, 11, 13),
                        LocalDate.of(2025, 11, 14),
                        LocalDate.of(2025, 11, 15))));

        final SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(allScalarsTable)
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
        } finally {
            release(response);
        }
    }

    @Test
    public void snapshotAllArrayTypes() {
        // Test all array types
        final TableReference allArraysTable = ref(TableTools.newTable(
                // Primitive array types
                col("byteArrayCol",
                        new byte[] {1, 2, 3},
                        new byte[] {4, 5, 6},
                        new byte[] {7, 8, 9}),
                col("shortArrayCol",
                        new short[] {100, 200},
                        new short[] {300, 400},
                        new short[] {500, 600}),
                col("intArrayCol",
                        new int[] {1000, 2000, 3000},
                        new int[] {4000, 5000, 6000},
                        new int[] {7000, 8000, 9000}),
                col("longArrayCol",
                        new long[] {100000L, 200000L},
                        new long[] {300000L, 400000L},
                        new long[] {500000L, 600000L}),
                col("floatArrayCol",
                        new float[] {1.5f, 2.5f},
                        new float[] {3.5f, 4.5f},
                        new float[] {5.5f, 6.5f}),
                col("doubleArrayCol",
                        new double[] {1.1, 2.2, 3.3},
                        new double[] {4.4, 5.5, 6.6},
                        new double[] {7.7, 8.8, 9.9}),
                col("booleanArrayCol",
                        new Boolean[] {true, false},
                        new Boolean[] {false, true},
                        new Boolean[] {true, true}),
                col("charArrayCol",
                        new char[] {'a', 'b', 'c'},
                        new char[] {'d', 'e', 'f'},
                        new char[] {'g', 'h', 'i'}),

                // String array type
                col("stringArrayCol",
                        new String[] {"hello", "world"},
                        new String[] {"foo", "bar", "baz"},
                        new String[] {"test"}),

                // Temporal array types
                col("instantArrayCol",
                        new Instant[] {
                                Instant.parse("2025-01-01T10:00:00Z"),
                                Instant.parse("2025-01-01T11:00:00Z")
                        },
                        new Instant[] {
                                Instant.parse("2025-01-02T10:00:00Z")
                        },
                        new Instant[] {
                                Instant.parse("2025-01-03T10:00:00Z"),
                                Instant.parse("2025-01-03T11:00:00Z"),
                                Instant.parse("2025-01-03T12:00:00Z")
                        }),
                col("localTimeArrayCol",
                        new LocalTime[] {LocalTime.of(10, 0), LocalTime.of(11, 0)},
                        new LocalTime[] {LocalTime.of(12, 0), LocalTime.of(13, 0)},
                        new LocalTime[] {LocalTime.of(14, 0), LocalTime.of(15, 0)}),
                col("localDateArrayCol",
                        new LocalDate[] {LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 2)},
                        new LocalDate[] {LocalDate.of(2025, 2, 1), LocalDate.of(2025, 2, 2)},
                        new LocalDate[] {LocalDate.of(2025, 3, 1), LocalDate.of(2025, 3, 2)})));

        final SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(2))
                .setSourceId(allArraysTable)
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
        } finally {
            release(response);
        }
    }

    @Test
    public void snapshotWithNullValues() {
        // Test handling of null values in all types
        final TableReference nullsTable = ref(TableTools.newTable(
                col("byteColWithNull", (byte) 1, null, (byte) 3),
                col("intColWithNull", 100, null, 300),
                col("doubleColWithNull", 1.1, null, 3.3),
                col("stringColWithNull", "a", null, "c"),
                col("instantColWithNull",
                        Instant.parse("2025-01-01T10:00:00Z"),
                        null,
                        Instant.parse("2025-01-03T12:00:00Z")),
                col("localTimeColWithNull",
                        LocalTime.of(10, 30, 45),
                        null,
                        LocalTime.of(22, 45, 0)),
                col("localDateColWithNull",
                        LocalDate.of(2025, 11, 13),
                        null,
                        LocalDate.of(2025, 11, 15)),
                col("stringArrayColWithNull",
                        new String[] {"a", "b"},
                        null,
                        new String[] {"c"}),
                col("instantArrayColWithNull",
                        new Instant[] {Instant.parse("2025-01-01T10:00:00Z")},
                        null,
                        new Instant[] {Instant.parse("2025-01-03T12:00:00Z")}),
                col("localTimeArrayColWithNull",
                        new LocalTime[] {LocalTime.of(10, 0)},
                        null,
                        new LocalTime[] {LocalTime.of(14, 0)}),
                col("localDateArrayColWithNull",
                        new LocalDate[] {LocalDate.of(2025, 1, 1)},
                        null,
                        new LocalDate[] {LocalDate.of(2025, 3, 1)})));

        final SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(3))
                .setSourceId(nullsTable)
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
        } finally {
            release(response);
        }
    }

    @Test
    public void snapshotMixedTypesLargeDataset() {
        // Test with a larger dataset containing mixed types
        final int rowCount = 100;
        final Object[] byteValues = new Object[rowCount];
        final Object[] intValues = new Object[rowCount];
        final Object[] doubleValues = new Object[rowCount];
        final Object[] stringValues = new Object[rowCount];
        final Object[] localTimeValues = new Object[rowCount];
        final Object[] localDateValues = new Object[rowCount];

        for (int i = 0; i < rowCount; i++) {
            byteValues[i] = (byte) (i % 128);
            intValues[i] = i * 1000;
            doubleValues[i] = i * 3.14;
            stringValues[i] = "row_" + i;
            localTimeValues[i] = LocalTime.of(i % 24, i % 60, 0);
            localDateValues[i] = LocalDate.of(2025, 1 + (i % 12), 1 + (i % 28));
        }

        final TableReference largeTable = ref(TableTools.newTable(
                col("byteCol", byteValues),
                col("intCol", intValues),
                col("doubleCol", doubleValues),
                col("stringCol", stringValues),
                col("localTimeCol", localTimeValues),
                col("localDateCol", localDateValues)));

        final SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(4))
                .setSourceId(largeTable)
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
        } finally {
            release(response);
        }
    }
}

