//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.MultiJoinInput;
import io.deephaven.proto.backplane.grpc.MultiJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiJoinGrpcTest extends GrpcTableOperationTestBase<MultiJoinTablesRequest> {

    @Override
    public ExportedTableCreationResponse send(MultiJoinTablesRequest request) {
        return channel().tableBlocking().multiJoinTables(request);
    }

    @Test
    public void multiJoinStatic() {
        final MultiJoinTablesRequest request = prototype();
        final ExportedTableCreationResponse response = channel().tableBlocking().multiJoinTables(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(1);
        } finally {
            release(response);
        }
    }

    @Test
    public void missingResultId() {
        final MultiJoinTablesRequest request = MultiJoinTablesRequest.newBuilder(prototype())
                .clearResultId()
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void zeroTables() {
        final MultiJoinTablesRequest request = MultiJoinTablesRequest.newBuilder(prototype())
                .clearMultiJoinInputs()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "Cannot join zero source tables.");
    }

    @Test
    public void columnsToMatchNotProvided() {
        final TableReference t1 = ref(TableTools.emptyTable(1).view("Key=ii", "First=ii"));
        final TableReference t2 = ref(TableTools.emptyTable(1).view("Key=ii", "Second=ii*2"));

        final MultiJoinInput input1 = MultiJoinInput.newBuilder()
                .setSourceId(t1)
                .addColumnsToMatch("OutputKey=Key")
                .addColumnsToAdd("First")
                .build();
        final MultiJoinInput input2 = MultiJoinInput.newBuilder()
                .setSourceId(t2)
                .build();

        final MultiJoinTablesRequest request = MultiJoinTablesRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .addMultiJoinInputs(input1)
                .addMultiJoinInputs(input2)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "must have at least one columns_to_match");
    }

    private MultiJoinTablesRequest prototype() {
        final TableReference t1 = ref(TableTools.emptyTable(1).view("Key=ii", "First=ii"));
        final TableReference t2 = ref(TableTools.emptyTable(1).view("Key=ii", "Second=ii*2"));
        final TableReference t3 = ref(TableTools.emptyTable(1).view("Key=ii", "Third=ii*3"));

        final MultiJoinInput input1 = MultiJoinInput.newBuilder()
                .setSourceId(t1)
                .addColumnsToMatch("OutputKey=Key")
                .addColumnsToAdd("First")
                .build();
        final MultiJoinInput input2 = MultiJoinInput.newBuilder()
                .setSourceId(t2)
                .addColumnsToMatch("OutputKey=Key")
                .addColumnsToAdd("Second")
                .build();
        final MultiJoinInput input3 = MultiJoinInput.newBuilder()
                .setSourceId(t3)
                .addColumnsToMatch("OutputKey=Key")
                .addColumnsToAdd("Third")
                .build();

        return MultiJoinTablesRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .addMultiJoinInputs(input1)
                .addMultiJoinInputs(input2)
                .addMultiJoinInputs(input3)
                .build();
    }
}
