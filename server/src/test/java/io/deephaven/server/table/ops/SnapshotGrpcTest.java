package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.SnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public final class SnapshotGrpcTest extends GrpcTableOperationTestBase<SnapshotTableRequest> {

    @Override
    public ExportedTableCreationResponse send(SnapshotTableRequest request) {
        return channel().tableBlocking().snapshot(request);
    }

    @Test
    public void singleSnapshot() {
        final TableReference timeTable = ref(TableTools.timeTable("00:00:01"));
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
    public void missingResultId() {
        final TableReference timeTable = ref(TableTools.timeTable("00:00:01"));
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
