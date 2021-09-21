package io.deephaven.grpc_api.runner;

import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

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
}
