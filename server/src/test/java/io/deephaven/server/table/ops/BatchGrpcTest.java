//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

public class BatchGrpcTest extends GrpcTableOperationTestBase<BatchTableRequest> {

    @Override
    public ExportedTableCreationResponse send(BatchTableRequest request) {
        throw new UnsupportedOperationException("batch returns stream");
    }

    @Test
    public void batchExample() {
        // empty_table(100).view("I=ii")
        final BatchTableRequest request = BatchTableRequest.newBuilder()
                .addOps(Operation.newBuilder()
                        .setEmptyTable(EmptyTableRequest.newBuilder()
                                .setSize(100)
                                .build())
                        .build())
                .addOps(Operation.newBuilder()
                        .setView(SelectOrUpdateRequest.newBuilder()
                                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                                .setSourceId(TableReference.newBuilder().setBatchOffset(0).build())
                                .addColumnSpecs("I=ii")
                                .build())
                        .build())
                .build();
        final Iterator<ExportedTableCreationResponse> it = channel().tableBlocking().batch(request);
        assertThat(it).hasNext();
        {
            final ExportedTableCreationResponse export = it.next();
            assertThat(export.getSuccess()).isTrue();
            assertThat(export.getIsStatic()).isTrue();
            assertThat(export.getSize()).isEqualTo(100);
        }
        assertThat(it).hasNext();
        {
            final ExportedTableCreationResponse export = it.next();
            assertThat(export.getSuccess()).isTrue();
            assertThat(export.getIsStatic()).isTrue();
            assertThat(export.getSize()).isEqualTo(100);
        }
        assertThat(it).isExhausted();
    }

    @Test
    public void empty() {
        final Iterator<ExportedTableCreationResponse> it =
                channel().tableBlocking().batch(BatchTableRequest.getDefaultInstance());
        try {
            it.next();
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT);
            assertThat(e.getMessage())
                    .contains("io.deephaven.proto.backplane.grpc.BatchTableRequest must have at least one ops (1)");
        }
    }

    @Test
    public void emptyOp() {
        final Iterator<ExportedTableCreationResponse> it =
                channel().tableBlocking().batch(BatchTableRequest.newBuilder()
                        .addOps(Operation.getDefaultInstance())
                        .build());
        try {
            it.next();
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT);
            assertThat(e.getMessage()).contains(
                    "io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation must have oneof op. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.");
        }
    }
}
