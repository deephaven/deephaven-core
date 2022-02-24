package io.deephaven.server.runner;

import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class OperationsTest extends DeephavenApiServerSingleAuthenticatedBase {

    @Test
    public void emptyTable() {
        ExportedTableCreationResponse response = channel.tableBlocking()
                .emptyTable(EmptyTableRequest.newBuilder().setResultId(id(1)).setSize(123).build());
        checkResponse(response, id(1), true, 123);
    }

    @Test
    public void timeTable() {
        ExportedTableCreationResponse response = channel.tableBlocking()
                .timeTable(TimeTableRequest.newBuilder().setResultId(id(1)).setPeriodNanos(TimeUnit.SECONDS.toNanos(1))
                        .build());
        checkResponse(response, id(1), false, 0);
    }

    @Test
    public void timeTableInvalid() {
        try {
            channel.tableBlocking().timeTable(invalidTimeTable(id(1)));
            failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Code.FAILED_PRECONDITION);
            assertThat(e.getStatus().getDescription()).isEqualTo("periodNanos must be >= 0 (found: -1000000000)");
        }
    }

    @Test
    public void batchTimeTableInvalid() {
        try {
            channel.tableBlocking().batch(batch(invalidTimeTable(id(1)))).next();
            failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Code.FAILED_PRECONDITION);
            assertThat(e.getStatus().getDescription()).isEqualTo("periodNanos must be >= 0 (found: -1000000000)");
        }
    }

    // TODO(deephaven-core#1333): Expand "integration" tests to cover all gRPC methods

    static void checkResponse(ExportedTableCreationResponse response, Ticket ticket, boolean isStatic, long size) {
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getErrorInfo()).isEmpty();
        assertThat(response.getResultId().hasTicket()).isTrue();
        assertThat(response.getResultId().getTicket()).isEqualTo(ticket);
        assertThat(response.getIsStatic()).isEqualTo(isStatic);
        assertThat(response.getSize()).isEqualTo(size);
    }

    private static Ticket id(int exportId) {
        return ExportTicketHelper.wrapExportIdInTicket(exportId);
    }

    private static Operation batchOperation(TimeTableRequest request) {
        return Operation.newBuilder().setTimeTable(request).build();
    }

    private static BatchTableRequest batch(TimeTableRequest request) {
        return BatchTableRequest.newBuilder().addOps(batchOperation(request)).build();
    }

    private static TimeTableRequest invalidTimeTable(Ticket id) {
        return TimeTableRequest.newBuilder().setResultId(id).setPeriodNanos(TimeUnit.SECONDS.toNanos(-1)).build();
    }
}
