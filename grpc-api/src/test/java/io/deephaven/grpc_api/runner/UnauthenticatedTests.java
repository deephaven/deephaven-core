package io.deephaven.grpc_api.runner;

import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.grpc.Status;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class UnauthenticatedTests extends DeephavenApiServerSingleUnauthenticatedBase {

    @Test
    public void emptyTable() {
        checkUnauthenticated(() -> {
            Ticket resultId = id1();
            channel.tableBlocking()
                    .emptyTable(EmptyTableRequest.newBuilder().setSize(123).setResultId(resultId).build());
        });
    }

    @Test
    public void timeTable() {
        checkUnauthenticated(() -> {
            Ticket resultId = id1();
            channel.tableBlocking()
                    .timeTable(TimeTableRequest.newBuilder().setPeriodNanos(TimeUnit.SECONDS.toNanos(1))
                            .setResultId(resultId).build());
        });
    }

    // todo: exercise all requests unauthenticated

    private static Ticket id1() {
        return ExportTicketHelper.wrapExportIdInTicket(1);
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
