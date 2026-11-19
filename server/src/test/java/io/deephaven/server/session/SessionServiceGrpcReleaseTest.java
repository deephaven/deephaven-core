//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.deephaven.util.SafeCloseable;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Drives the release RPC against a live session, covering ids that do not exist yet, ids already released, and
 * server-allocated ids.
 */
public class SessionServiceGrpcReleaseTest extends DeephavenApiServerSingleAuthenticatedBase {

    private SafeCloseable executionContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    @Override
    public void tearDown() throws Exception {
        if (executionContext != null) {
            executionContext.close();
            executionContext = null;
        }
        super.tearDown();
    }

    @Test
    public void releaseBeforeCreateSucceeds() {
        // a client whose release overtakes its create must not be told the release failed
        release(id(1));
    }

    @Test
    public void createAfterReleaseIsRejected() {
        release(id(1));

        assertThatThrownBy(() -> emptyTable(id(1)))
                .as("create at an id released before it was created")
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode())
                                .as("status of a create at an id released before it was created")
                                .isEqualTo(Code.CANCELLED));
        assertThat(authenticatedSessionState().getExport(1).getState())
                .as("state of an id released before it was created, after a create attempt")
                .isEqualTo(ExportNotification.State.CANCELLED);
    }

    @Test
    public void doubleReleaseBeforeCreateSucceeds() {
        release(id(1));
        release(id(1));
    }

    @Test
    public void doubleReleaseOfExportedSucceeds() {
        emptyTable(id(1));
        release(id(1));
        release(id(1));
        assertThat(authenticatedSessionState().getExport(1).getState())
                .isEqualTo(ExportNotification.State.RELEASED);
    }

    @Test
    public void releaseServerSideExport() {
        final SessionState session = authenticatedSessionState();
        final int before = session.numExports();
        final SessionState.ExportObject<Table> export = session.newServerSideExport(TableTools.emptyTable(1));

        release(export.getExportId());
        assertThat(export.getState()).isEqualTo(ExportNotification.State.RELEASED);
        assertThat(session.numExports()).isEqualTo(before);

        release(export.getExportId());
    }

    @Test
    public void releaseOfUnallocatedServerSideIdFailsAndDoesNotPoisonAllocation() {
        // server ids count down from -1; find the next one by allocating its predecessor
        final SessionState session = authenticatedSessionState();
        final SessionState.ExportObject<Table> first = session.newServerSideExport(TableTools.emptyTable(1));
        final int nextServerId = ExportTicketHelper.ticketToExportId(first.getExportId(), "first") - 1;

        assertThatThrownBy(() -> release(id(nextServerId)))
                .as("release of a server id that was never allocated")
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode())
                                .as("status of a release that can never succeed")
                                .isEqualTo(Code.FAILED_PRECONDITION));

        // the server must still be able to hand out that id
        final SessionState.ExportObject<Table> second = session.newServerSideExport(TableTools.emptyTable(1));
        assertThat(second.getExportId()).isEqualTo(id(nextServerId));
        assertThat(second.getState()).isEqualTo(ExportNotification.State.EXPORTED);

        first.cancel();
        second.cancel();
    }

    @Test
    public void releaseOfNonExportIdIsInvalidArgument() {
        assertThatThrownBy(() -> release(id(SessionState.NON_EXPORT_ID)))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode())
                                .as("status of a release of the non-export id")
                                .isEqualTo(Code.INVALID_ARGUMENT));
    }

    private void release(final Ticket ticket) {
        // noinspection ResultOfMethodCallIgnored
        channel().sessionBlocking().release(ReleaseRequest.newBuilder().setId(ticket).build());
    }

    private void emptyTable(final Ticket ticket) {
        // noinspection ResultOfMethodCallIgnored
        channel().tableBlocking().emptyTable(EmptyTableRequest.newBuilder().setResultId(ticket).setSize(1).build());
    }

    private static Ticket id(final int exportId) {
        return ExportTicketHelper.wrapExportIdInTicket(exportId);
    }
}
