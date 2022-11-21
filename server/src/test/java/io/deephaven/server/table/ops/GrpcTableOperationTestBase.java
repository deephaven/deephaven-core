package io.deephaven.server.table.ops;

import com.google.protobuf.Message;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.util.SafeCloseable;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public abstract class GrpcTableOperationTestBase<Request extends Message>
        extends DeephavenApiServerSingleAuthenticatedBase {

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

    public abstract ExportedTableCreationResponse send(Request request);

    public void assertError(Request request, Code code, String message) {
        final ExportedTableCreationResponse response;
        try {
            response = send(request);
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(code);
            assertThat(e).hasMessageContaining(message);
            return;
        }
        release(response);
        failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
    }

    public void release(ExportedTableCreationResponse exportedTableCreationResponse) {
        release(exportedTableCreationResponse.getResultId().getTicket());
    }

    public void release(Ticket ticket) {
        // noinspection ResultOfMethodCallIgnored
        channel().sessionBlocking().release(ReleaseRequest.newBuilder().setId(ticket).build());
    }

    public static TableReference ref(ExportObject<?> export) {
        return TableReference.newBuilder().setTicket(export.getExportId()).build();
    }
}
