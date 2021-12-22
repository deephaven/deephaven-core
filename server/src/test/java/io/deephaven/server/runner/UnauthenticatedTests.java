package io.deephaven.server.runner;

import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class UnauthenticatedTests extends DeephavenApiServerSingleUnauthenticatedBase {

    @Test
    public void emptyTable() {
        checkUnauthenticated(() -> {
            Ticket resultId = id(1);
            channel.tableBlocking()
                    .emptyTable(EmptyTableRequest.newBuilder().setSize(123).setResultId(resultId).build());
        });
    }

    @Test
    public void timeTable() {
        checkUnauthenticated(() -> {
            Ticket resultId = id(1);
            channel.tableBlocking()
                    .timeTable(TimeTableRequest.newBuilder().setPeriodNanos(TimeUnit.SECONDS.toNanos(1))
                            .setResultId(resultId).build());
        });
    }

    // TODO(deephaven-core#1333): Expand "integration" tests to cover all gRPC methods

    private static Ticket id(int exportId) {
        return ExportTicketHelper.wrapExportIdInTicket(exportId);
    }

    static void checkUnauthenticated(Runnable r) {
        try {
            r.run();
        } catch (io.grpc.StatusRuntimeException e) {
            if (e.getStatus() == Status.UNAUTHENTICATED) {
                // expected
                return;
            }
        }
        Assertions.fail("Expected UNAUTHENTICATED exception");
    }
}
