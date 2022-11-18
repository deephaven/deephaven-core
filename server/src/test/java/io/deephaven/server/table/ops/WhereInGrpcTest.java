package io.deephaven.server.table.ops;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.WhereInRequest;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.util.SafeCloseable;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class WhereInGrpcTest extends DeephavenApiServerSingleAuthenticatedBase {

    private SafeCloseable executionContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        executionContext = ExecutionContext.createForUnitTests().open();
    }

    @Override
    public void tearDown() throws Exception {
        executionContext.close();
        super.tearDown();
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

    private void assertError(WhereInRequest request, Code code, String message) {
        final ExportedTableCreationResponse response;
        try {
            response = channel().tableBlocking().whereIn(request);
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(code);
            assertThat(e).hasMessageContaining(message);
            return;
        }
        release(response);
        failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
    }

    private void release(ExportedTableCreationResponse exportedTableCreationResponse) {
        release(exportedTableCreationResponse.getResultId().getTicket());
    }

    private void release(Ticket ticket) {
        // noinspection ResultOfMethodCallIgnored
        channel().sessionBlocking().release(ReleaseRequest.newBuilder().setId(ticket).build());
    }

    private static TableReference ref(ExportObject<?> export) {
        return TableReference.newBuilder().setTicket(export.getExportId()).build();
    }
}
