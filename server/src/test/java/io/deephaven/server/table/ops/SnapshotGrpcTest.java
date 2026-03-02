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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

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
                col("longCol", 1000L, 2000L, 3000L),
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
                col("bigIntegerCol",
                        BigInteger.valueOf(123456789L),
                        null,
                        BigInteger.valueOf(987654321L)),
                col("bigDecimalCol",
                        new BigDecimal("123.456"),
                        null,
                        new BigDecimal("789.012")),
                col("intArrayCol",
                        new int[] {1, 2, 3},
                        null,
                        new int[] {6}),
                col("stringArrayCol",
                        new String[] {"a", "b"},
                        null,
                        new String[] {"c"}),
                col("localTimeArrayCol",
                        new LocalTime[] {LocalTime.of(9, 0)},
                        null,
                        new LocalTime[] {LocalTime.of(18, 0)}),
                col("localDateArrayCol",
                        new LocalDate[] {LocalDate.of(2025, 1, 15)},
                        null,
                        new LocalDate[] {LocalDate.of(2025, 12, 25)})));

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
