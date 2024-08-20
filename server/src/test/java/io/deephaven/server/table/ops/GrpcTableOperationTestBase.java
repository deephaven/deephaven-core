//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.protobuf.Message;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.util.SafeCloseable;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public abstract class GrpcTableOperationTestBase<Request extends Message>
        extends DeephavenApiServerSingleAuthenticatedBase {

    private SafeCloseable executionContext;

    private List<ExportObject<?>> exports;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        executionContext = TestExecutionContext.createForUnitTests().open();
        exports = new ArrayList<>();
    }

    @Override
    public void tearDown() throws Exception {
        if (exports != null) {
            for (ExportObject<?> export : exports) {
                export.cancel();
            }
            exports = null;
        }
        if (executionContext != null) {
            executionContext.close();
            executionContext = null;
        }
        super.tearDown();
    }

    public TableReference ref(Table table) {
        final ExportObject<Table> export = authenticatedSessionState().newServerSideExport(table);
        exports.add(export);
        return ref(export);
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
