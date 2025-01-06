//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.SliceRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SliceGrpcTest extends GrpcTableOperationTestBase<SliceRequest> {

    @Override
    public ExportedTableCreationResponse send(SliceRequest request) {
        return channel().tableBlocking().slice(request);
    }

    @Test
    public void missingResultId() {
        final SliceRequest request = SliceRequest.newBuilder()
                .clearResultId()
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingSourceId() {
        final SliceRequest request = SliceRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.SliceRequest must have field source_id (2)");
    }

    @Test
    public void bothNonNegativeStartBeforeEnd() {
        final TableReference emptyTable = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SliceRequest request = SliceRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(emptyTable)
                .setFirstPositionInclusive(0)
                .setLastPositionExclusive(2)
                .build();
        final ExportedTableCreationResponse response = send(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(2);
    }

    @Test
    public void bothNonNegativeStartAfterEnd() {
        final TableReference emptyTable = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SliceRequest request = SliceRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(emptyTable)
                .setFirstPositionInclusive(2)
                .setLastPositionExclusive(0)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "Cannot slice with a non-negative start position that is after a non-negative end position.");
    }

    @Test
    public void bothNegativeStartBeforeEnd() {
        final TableReference emptyTable = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SliceRequest request = SliceRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(emptyTable)
                .setFirstPositionInclusive(-2)
                .setLastPositionExclusive(-1)
                .build();
        final ExportedTableCreationResponse response = send(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(1);
    }

    @Test
    public void bothNegativeStartAfterEnd() {
        final TableReference emptyTable = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SliceRequest request = SliceRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(emptyTable)
                .setFirstPositionInclusive(-1)
                .setLastPositionExclusive(-2)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "Cannot slice with a negative start position that is after a negative end position.");
    }

    @Test
    public void diffSignStartBeforeEnd() {
        final TableReference emptyTable = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SliceRequest request = SliceRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(emptyTable)
                .setFirstPositionInclusive(-2)
                .setLastPositionExclusive(2)
                .build();
        final ExportedTableCreationResponse response = send(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(0);
    }

    @Test
    public void diffSignStartAfterEnd() {
        final TableReference emptyTable = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SliceRequest request = SliceRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(emptyTable)
                .setFirstPositionInclusive(2)
                .setLastPositionExclusive(-2)
                .build();
        final ExportedTableCreationResponse response = send(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(1);
    }

}
