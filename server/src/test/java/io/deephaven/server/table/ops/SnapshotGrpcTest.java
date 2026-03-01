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

import java.time.Instant;
import java.time.LocalTime;
import java.time.LocalDate;

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
    public void snapshotLocalTimeAndLocalDate() {
        // Test LocalTime and LocalDate types - validates they serialize/deserialize correctly
        final TableReference table = ref(TableTools.newTable(
                col("localTimeCol",
                        LocalTime.of(10, 30, 45),
                        LocalTime.of(14, 15, 30),
                        LocalTime.of(22, 45, 0)),
                col("localDateCol",
                        LocalDate.of(2025, 11, 13),
                        LocalDate.of(2025, 11, 14),
                        LocalDate.of(2025, 11, 15)),
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
    public void snapshotWithLocalTimeAndLocalDateNulls() {
        // Test null handling for LocalTime and LocalDate
        final TableReference table = ref(TableTools.newTable(
                col("localTimeWithNull",
                        LocalTime.of(10, 30),
                        null,
                        LocalTime.of(20, 45)),
                col("localDateWithNull",
                        LocalDate.of(2025, 1, 1),
                        null,
                        LocalDate.of(2025, 12, 31))));

        final SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(3))
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
    public void snapshotAllTypesIncludingLocalTimeAndLocalDate() {
        // Comprehensive test with all supported types
        final TableReference table = ref(TableTools.newTable(
                col("byteCol", (byte) 1, (byte) 2, (byte) 3),
                col("intCol", 100, 200, 300),
                col("longCol", 1000L, 2000L, 3000L),
                col("doubleCol", 1.1, 2.2, 3.3),
                col("stringCol", "a", "b", "c"),
                col("instantCol",
                        Instant.parse("2025-01-01T10:00:00Z"),
                        Instant.parse("2025-01-02T11:00:00Z"),
                        Instant.parse("2025-01-03T12:00:00Z")),
                col("localTimeCol",
                        LocalTime.of(10, 0),
                        LocalTime.of(14, 0),
                        LocalTime.of(22, 0)),
                col("localDateCol",
                        LocalDate.of(2025, 1, 1),
                        LocalDate.of(2025, 6, 15),
                        LocalDate.of(2025, 12, 31)),
                col("stringArrayCol",
                        new String[] {"x", "y"},
                        new String[] {"a", "b", "c"},
                        new String[] {"foo"}),
                col("localTimeArrayCol",
                        new LocalTime[] {LocalTime.of(9, 0)},
                        new LocalTime[] {LocalTime.of(12, 0), LocalTime.of(13, 0)},
                        new LocalTime[] {LocalTime.of(18, 0)}),
                col("localDateArrayCol",
                        new LocalDate[] {LocalDate.of(2025, 1, 15)},
                        new LocalDate[] {LocalDate.of(2025, 6, 1), LocalDate.of(2025, 6, 30)},
                        new LocalDate[] {LocalDate.of(2025, 12, 25)})));

        final SnapshotTableRequest request = SnapshotTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(4))
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
