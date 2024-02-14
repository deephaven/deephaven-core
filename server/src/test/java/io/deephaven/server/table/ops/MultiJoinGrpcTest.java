/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
    public void multiJoinStaticFromInputs() {
        final MultiJoinTablesRequest request = prototypeMultiJoinInputs();
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
                .clearSourceIds()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "Cannot join zero source tables.");
    }

    @Test
    public void sourceTablesAndInputProvided() {
        MultiJoinTablesRequest prototype = prototype();
        final MultiJoinTablesRequest request = MultiJoinTablesRequest.newBuilder(prototypeMultiJoinInputs())
                .addAllSourceIds(prototype.getSourceIdsList())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "If `multi_join_inputs` are provided, `source_ids` must remain empty.");
    }

    @Test
    public void columnsToMatchAndInputProvided() {
        final MultiJoinTablesRequest request = MultiJoinTablesRequest.newBuilder(prototypeMultiJoinInputs())
                .addColumnsToMatch("OutputKey=Key")
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "If `multi_join_inputs` are provided, `columns_to_match` must remain empty.");
    }

    private MultiJoinTablesRequest prototype() {
        final TableReference t1 = ref(TableTools.emptyTable(1).view("Key=ii", "First=ii"));
        final TableReference t2 = ref(TableTools.emptyTable(1).view("Key=ii", "Second=ii*2"));
        final TableReference t3 = ref(TableTools.emptyTable(1).view("Key=ii", "Third=ii*3", "Extra=ii*4"));
        return MultiJoinTablesRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .addSourceIds(t1)
                .addSourceIds(t2)
                .addSourceIds(t3)
                .addColumnsToMatch("OutputKey=Key")
                .build();
    }

    private MultiJoinTablesRequest prototypeMultiJoinInputs() {
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
