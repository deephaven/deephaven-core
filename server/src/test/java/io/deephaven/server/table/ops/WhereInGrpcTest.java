package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.WhereInRequest;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.session.SessionState.ExportObject;
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
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(1).view("Id=0"));
        try {
            final WhereInRequest request = WhereInRequest.newBuilder()
                    .setLeftId(ref(emptyTable))
                    .setRightId(ref(emptyTable))
                    .addColumnsToMatch("Id")
                    .build();
            assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void missingLeftId() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(0).view("Id=0"));
        try {
            final WhereInRequest request = WhereInRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setRightId(ref(emptyTable))
                    .addColumnsToMatch("Id")
                    .build();
            assertError(request, Code.INVALID_ARGUMENT,
                    "io.deephaven.proto.backplane.grpc.WhereInRequest must have field left_id (2)");
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void missingRightId() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(0).view("Id=0"));
        try {
            final WhereInRequest request = WhereInRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setLeftId(ref(emptyTable))
                    .addColumnsToMatch("Id")
                    .build();
            assertError(request, Code.INVALID_ARGUMENT,
                    "io.deephaven.proto.backplane.grpc.WhereInRequest must have field right_id (3)");
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void missingColumns() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(0).view("Id=0"));
        try {
            final WhereInRequest request = WhereInRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setLeftId(ref(emptyTable))
                    .setRightId(ref(emptyTable))
                    .build();
            assertError(request, Code.INVALID_ARGUMENT,
                    "io.deephaven.proto.backplane.grpc.WhereInRequest must have at least one columns_to_match (5)");
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void whereInStatic() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(1).view("Id=0"));
        try {
            final WhereInRequest request = WhereInRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setLeftId(ref(emptyTable))
                    .setRightId(ref(emptyTable))
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
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void whereNotInStatic() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(1).view("Id=0"));
        try {
            final WhereInRequest request = WhereInRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setLeftId(ref(emptyTable))
                    .setRightId(ref(emptyTable))
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
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void whereInTicking() {
        final ExportObject<Table> ticking = authenticatedSessionState()
                .newServerSideExport(TableTools.timeTable("00:00:01").view("Id=ii"));
        try {
            final WhereInRequest request = WhereInRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setLeftId(ref(ticking))
                    .setRightId(ref(ticking))
                    .addColumnsToMatch("Id")
                    .build();
            final ExportedTableCreationResponse response = channel().tableBlocking().whereIn(request);
            try {
                assertThat(response.getSuccess()).isTrue();
                assertThat(response.getIsStatic()).isFalse();
            } finally {
                release(response);
            }
        } finally {
            ticking.cancel();
        }
    }

    @Test
    public void whereInLeftTicking() {
        final ExportObject<Table> ticking = authenticatedSessionState()
                .newServerSideExport(TableTools.timeTable("00:00:01").view("Id=ii"));
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(1).view("Id=0"));
        try {
            final WhereInRequest request = WhereInRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setLeftId(ref(ticking))
                    .setRightId(ref(emptyTable))
                    .addColumnsToMatch("Id")
                    .build();
            final ExportedTableCreationResponse response = channel().tableBlocking().whereIn(request);
            try {
                assertThat(response.getSuccess()).isTrue();
                assertThat(response.getIsStatic()).isFalse();
            } finally {
                release(response);
            }
        } finally {
            ticking.cancel();
            emptyTable.cancel();
        }
    }

    @Test
    public void whereInRightTicking() {
        final ExportObject<Table> ticking = authenticatedSessionState()
                .newServerSideExport(TableTools.timeTable("00:00:01").view("Id=ii"));
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(1).view("Id=0"));
        try {
            final WhereInRequest request = WhereInRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setLeftId(ref(emptyTable))
                    .setRightId(ref(ticking))
                    .addColumnsToMatch("Id")
                    .build();
            final ExportedTableCreationResponse response = channel().tableBlocking().whereIn(request);
            try {
                assertThat(response.getSuccess()).isTrue();
                assertThat(response.getIsStatic()).isFalse();
            } finally {
                release(response);
            }
        } finally {
            ticking.cancel();
            emptyTable.cancel();
        }
    }
}
