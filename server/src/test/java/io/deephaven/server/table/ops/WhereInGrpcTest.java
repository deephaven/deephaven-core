//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.WhereInRequest;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class WhereInGrpcTest extends GrpcTableOperationTestBase<WhereInRequest> {

    @Override
    public ExportedTableCreationResponse send(WhereInRequest request) {
        return channel().tableBlocking().whereIn(request);
    }

    @Test
    public void missingResultId() {
        final TableReference emptyTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        final WhereInRequest request = WhereInRequest.newBuilder()
                .setLeftId(emptyTable)
                .setRightId(emptyTable)
                .addColumnsToMatch("Id")
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingLeftId() {
        final TableReference emptyTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        final WhereInRequest request = WhereInRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setRightId(emptyTable)
                .addColumnsToMatch("Id")
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.WhereInRequest must have field left_id (2)");
    }

    @Test
    public void missingRightId() {
        final TableReference emptyTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        final WhereInRequest request = WhereInRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(emptyTable)
                .addColumnsToMatch("Id")
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.WhereInRequest must have field right_id (3)");
    }

    @Test
    public void missingColumns() {
        final TableReference emptyTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        final WhereInRequest request = WhereInRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(emptyTable)
                .setRightId(emptyTable)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.WhereInRequest must have at least one columns_to_match (5)");
    }

    @Test
    public void unknownField() {
        final TableReference emptyTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        final WhereInRequest request = WhereInRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(emptyTable)
                .setRightId(emptyTable)
                .addColumnsToMatch("Id")
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(9999, Field.newBuilder().addFixed32(32).build())
                        .build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.WhereInRequest has unknown field(s)");
    }

    @Test
    public void whereInStatic() {
        final TableReference emptyTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        final WhereInRequest request = WhereInRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(emptyTable)
                .setRightId(emptyTable)
                .addColumnsToMatch("Id")
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().whereIn(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(1);
        } finally {
            release(response);
        }
    }

    @Test
    public void whereNotInStatic() {
        final TableReference emptyTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        final WhereInRequest request = WhereInRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(emptyTable)
                .setRightId(emptyTable)
                .addColumnsToMatch("Id")
                .setInverted(true)
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().whereIn(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(0);
        } finally {
            release(response);
        }
    }

    @Test
    public void whereInTicking() {
        final TableReference timeTable = ref(TableTools.timeTable("PT00:00:01").view("Id=ii"));
        final WhereInRequest request = WhereInRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(timeTable)
                .setRightId(timeTable)
                .addColumnsToMatch("Id")
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().whereIn(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        } finally {
            release(response);
        }
    }

    @Test
    public void whereInLeftTicking() {
        final TableReference timeTable = ref(TableTools.timeTable("PT00:00:01").view("Id=ii"));
        final TableReference emptyTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        final WhereInRequest request = WhereInRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(timeTable)
                .setRightId(emptyTable)
                .addColumnsToMatch("Id")
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().whereIn(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        } finally {
            release(response);
        }
    }

    @Test
    public void whereInRightTicking() {
        final TableReference timeTable = ref(TableTools.timeTable("PT00:00:01").view("Id=ii"));
        final TableReference emptyTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        final WhereInRequest request = WhereInRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(emptyTable)
                .setRightId(timeTable)
                .addColumnsToMatch("Id")
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().whereIn(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        } finally {
            release(response);
        }
    }
}
