package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.SingleSnapshotTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public final class SingleSnapshotGrpcTest extends GrpcTableOperationTestBase<SingleSnapshotTableRequest> {

    @Override
    public ExportedTableCreationResponse send(SingleSnapshotTableRequest request) {
        return channel().tableBlocking().singleSnapshot(request);
    }

    @Test
    public void singleSnapshot() {
        final TableReference timeTable = ref(TableTools.timeTable("00:00:01"));
        final SingleSnapshotTableRequest request = SingleSnapshotTableRequest.newBuilder()
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
        final SingleSnapshotTableRequest request = SingleSnapshotTableRequest.newBuilder()
                .setSourceId(timeTable)
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingSourceId() {
        final SingleSnapshotTableRequest request = SingleSnapshotTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.SingleSnapshotTableRequest must have field source_id (2)");
    }
}
