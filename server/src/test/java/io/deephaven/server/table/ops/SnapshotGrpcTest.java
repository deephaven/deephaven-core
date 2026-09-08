//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;

import static io.deephaven.engine.util.TableTools.col;
import static org.assertj.core.api.Assertions.assertThat;

public final class SnapshotGrpcTest extends GrpcTableOperationTestBase<SnapshotTableRequest> {

    @Override
    public ExportedTableCreationResponse send(SnapshotTableRequest request) {
        return channel().tableBlocking().snapshot(request);
    }

    @Test
    public void singleSnapshot() {
        final TableReference timeTable = ref(TableTools.timeTable("PT00:00:01"));
        final SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(timeTable)
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
    public void snapshotAllTypes() {
        // Comprehensive test covering all supported column types including nulls
        final TableReference table = ref(TableTools.newTable(
                col("booleanCol", true, false, true),
                col("byteCol", (byte) 1, (byte) 2, (byte) 3),
                col("charCol", 'a', 'b', 'c'),
                col("shortCol", (short) 10, (short) 20, (short) 30),
                col("intCol", 100, 200, 300),
                col("longCol", 1000L, 3000000000L, 3000L),
                col("floatCol", 1.1f, 2.2f, 3.3f),
                col("doubleCol", 1.1, 2.2, 3.3),
                col("stringCol", "hello", null, "world"),
                col("instantCol",
                        Instant.parse("2025-01-01T10:00:00Z"),
                        null,
                        Instant.parse("2025-01-03T12:00:00Z")),
                col("localTimeCol",
                        LocalTime.of(10, 30, 45),
                        null,
                        LocalTime.of(22, 45, 0)),
                col("localDateCol",
                        LocalDate.of(2025, 1, 1),
                        null,
                        LocalDate.of(2025, 12, 31)),
                col("durationCol",
                        Duration.ofHours(1),
                        null,
                        Duration.ofMinutes(30)),
                col("periodCol",
                        Period.ofDays(1),
                        null,
                        Period.ofMonths(2)),
                col("bigIntegerCol",
                        new BigInteger("12345678901234567890"),
                        null,
                        BigInteger.valueOf(987654321L)),
                col("bigDecimalCol",
                        new BigDecimal("12345678901234567890.123456789"),
                        null,
                        new BigDecimal("789.012")),
                col("booleanArrayCol",
                        new Boolean[] {true, false},
                        null,
                        new Boolean[] {true}),
                col("byteArrayCol",
                        new byte[] {1, 2},
                        null,
                        new byte[] {3}),
                col("charArrayCol",
                        new char[] {'a', 'b'},
                        null,
                        new char[] {'c'}),
                col("shortArrayCol",
                        new short[] {10, 20},
                        null,
                        new short[] {30}),
                col("intArrayCol",
                        new int[] {100, 200},
                        null,
                        new int[] {300}),
                col("longArrayCol",
                        new long[] {1000L, 3000000000L},
                        null,
                        new long[] {3000L}),
                col("floatArrayCol",
                        new float[] {1.1f, 2.2f},
                        null,
                        new float[] {3.3f}),
                col("doubleArrayCol",
                        new double[] {1.1, 2.2},
                        null,
                        new double[] {3.3}),
                col("stringArrayCol",
                        new String[] {"a", "b"},
                        null,
                        new String[] {"c"}),
                col("instantArrayCol",
                        new Instant[] {Instant.parse("2025-01-01T10:00:00Z")},
                        null,
                        new Instant[] {Instant.parse("2025-01-03T12:00:00Z")}),
                col("localTimeArrayCol",
                        new LocalTime[] {LocalTime.of(9, 0)},
                        null,
                        new LocalTime[] {LocalTime.of(18, 0)}),
                col("localDateArrayCol",
                        new LocalDate[] {LocalDate.of(2025, 1, 15)},
                        null,
                        new LocalDate[] {LocalDate.of(2025, 12, 25)}),
                col("durationArrayCol",
                        new Duration[] {Duration.ofHours(2)},
                        null,
                        new Duration[] {Duration.ofSeconds(45)}),
                col("periodArrayCol",
                        new Period[] {Period.ofWeeks(1)},
                        null,
                        new Period[] {Period.ofYears(1)}),
                col("bigIntegerArrayCol",
                        new BigInteger[] {new BigInteger("12345678901234567890")},
                        null,
                        new BigInteger[] {BigInteger.valueOf(456L)}),
                col("bigDecimalArrayCol",
                        new BigDecimal[] {new BigDecimal("12345678901234567890.123456789")},
                        null,
                        new BigDecimal[] {new BigDecimal("4.56")})));

        final SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(2))
                .setSourceId(table)
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
    public void missingResultId() {
        final TableReference timeTable = ref(TableTools.timeTable("PT00:00:01"));
        final SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
                .setSourceId(timeTable)
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingSourceId() {
        final SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.SnapshotTableRequest must have field source_id (2)");
    }
}
