//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.base.verify.Assert;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SharedId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableService;
import io.deephaven.client.impl.TicketId;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.util.function.ThrowingRunnable;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SessionPublishTest extends DeephavenSessionTestBase {
    private static final int TIMEOUT_MS = 50;

    private Session session2;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // Create a second client with its own channel and session.
        final ManagedChannel channel2 = channelBuilder().build();
        register(channel2);
        session2 = DaggerDeephavenSessionRoot.create()
                .factoryBuilder()
                .managedChannel(channel2)
                .scheduler(sessionScheduler)
                .build()
                .newSession();
    }

    @Override
    public void tearDown() throws Exception {
        session2.close();
        super.tearDown();
    }

    @Test
    public void testHandoff() throws Exception {
        final TimeTable sourceTable = TimeTable.of(Duration.ofSeconds(1));
        final TableHandle remoteSource = session.execute(sourceTable);
        final SharedId sharedId = SharedId.newRandom();

        // Let's publish the source table to the shared id.
        session.publish(sharedId, remoteSource).get();

        // Ensure that our destination is empty.
        final ScopeId destId = new ScopeId("destTestSharedId");
        try {
            session2.execute(destId.ticketId().table());
            // noinspection ThrowableNotThrown
            Assert.statementNeverExecuted();
        } catch (Exception err) {
            // expected
        }

        // Fetch a copy of the SharedId shared from the first client.
        final TableHandle client2Copy = session2.execute(sharedId.ticketId().table());

        // Close the first client's session.
        session.close();

        // Publish to the query scope via the second client's shared copy.
        session2.publish(destId, client2Copy).get();

        // Ensure that we can resolve from query scope.
        session2.execute(destId.ticketId().table());
    }

    @Test
    public void testFailsPublishingFromQueryScope() throws Exception {
        final TimeTable sourceTable = TimeTable.of(Duration.ofSeconds(1));
        final TableHandle remoteSource = session.execute(sourceTable);
        final ScopeId scopeId = new ScopeId("test_time_table");
        final SharedId sharedId = SharedId.newRandom();

        // Let's publish the source table to the query scope.
        session.publish(scopeId, remoteSource).get();

        // This publish should fail because the source is not a session owned export.
        try {
            session.publish(sharedId, scopeId).get();
            Assert.statementNeverExecuted();
        } catch (final ExecutionException err) {
            // This is expected.
            Assert.eqTrue(err.getCause() instanceof StatusRuntimeException,
                    "err.getCause() instanceof StatusRuntimeException");
        }
    }

    @Test
    public void testSharePublishCompletesImmediatelyThenPropagatesSuccess() throws Exception {
        final int futureExportId = 1024000; // something "in the future"
        final TicketId ticketId = new TicketId(ExportTicketHelper.exportIdToBytes(futureExportId));
        final SharedId sharedId = SharedId.newRandom();

        // Let's publish the source table to the shared id.
        session.publish(sharedId, ticketId).get();

        // Try to fetch a copy of the not-yet-resolved shared id.
        final TableService.TableHandleFuture tableFuture = session.executeAsync(sharedId.ticketId().table());
        expectTimeout(() -> tableFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        Assert.eqFalse(tableFuture.isDone(), "tableFuture.isDone()");

        // Define / Complete the source export.
        serverSessionState.newExport(futureExportId).submit(() -> TableTools.emptyTable(0));
        TableHandle tableHandle = tableFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Assert.eqTrue(tableHandle.isDone(), "tableHandle.isDone()");
        Assert.eqTrue(tableHandle.isSuccessful(), "tableHandle.isSuccessful()");
    }

    @Test
    public void testSharePublishCompletesImmediatelyThenPropagatesFailure() throws Exception {
        final int futureExportId = 1024000; // something "in the future"
        final TicketId ticketId = new TicketId(ExportTicketHelper.exportIdToBytes(futureExportId));
        final SharedId sharedId = SharedId.newRandom();

        // Let's publish the source table to the shared id.
        session.publish(sharedId, ticketId).get();

        // Try to fetch a copy of the not-yet-resolved shared id.
        final TableService.TableHandleFuture tableFuture = session.executeAsync(sharedId.ticketId().table());
        expectTimeout(() -> tableFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        Assert.eqFalse(tableFuture.isDone(), "tableFuture.isDone()");

        // Define / Complete the source export with a failure.
        serverSessionState.newExport(futureExportId).submit(() -> {
            throw new RuntimeException("test failure");
        });
        try {
            tableFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException err) {
            Assert.eqTrue(err.getCause() instanceof TableHandle.TableHandleException,
                    "err.getCause() instanceof TableHandle.TableHandleException");
        }
    }

    @Test
    public void testScopePublishWaitsForSourceCompletion() throws Exception {
        final int futureExportId = 1024000; // something "in the future"
        final TicketId ticketId = new TicketId(ExportTicketHelper.exportIdToBytes(futureExportId));
        final ScopeId scopeId = new ScopeId("test_dest_table");

        // Let's publish the source table to the shared id.
        final CompletableFuture<Void> rpc = session.publish(scopeId, ticketId);
        expectTimeout(() -> rpc.get(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        serverSessionState.newExport(futureExportId).submit(() -> TableTools.timeTable("PT1s"));

        // Now the RPC should complete immediately.
        rpc.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private static void expectTimeout(final ThrowingRunnable<Exception> runnable) {
        try {
            runnable.run();
            Assert.statementNeverExecuted();
        } catch (final Exception err) {
            // Only a timeout exception is expected / allowed.
            Assert.eqTrue(err instanceof TimeoutException, "err instanceof TimeoutException");
        }
    }
}
