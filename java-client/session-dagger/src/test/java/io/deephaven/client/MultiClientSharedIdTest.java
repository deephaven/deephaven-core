//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.base.verify.Assert;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.client.impl.SharedId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TicketId;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.qst.table.TimeTable;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MultiClientSharedIdTest extends DeephavenSessionTestBase {

    @Test
    public void testHandoff() throws Exception {
        final TimeTable sourceTable = TimeTable.of(Duration.ofSeconds(1));
        final TableHandle remoteSource = session.execute(sourceTable);
        final SharedId sharedId = SharedId.newRandom();

        // Let's publish the source table to the shared id.
        session.publish(sharedId, remoteSource).get();

        // Create a second client with its own channel and session.
        final ManagedChannel channel2 = channelBuilder().build();
        register(channel2);
        final SessionImpl client2 = DaggerDeephavenSessionRoot.create().factoryBuilder().managedChannel(channel2)
                .scheduler(sessionScheduler).build().newSession();

        // Ensure that our destination is empty.
        final ScopeId destId = new ScopeId("destTestSharedId");
        try {
            client2.execute(destId.ticketId().table());
            // noinspection ThrowableNotThrown
            Assert.statementNeverExecuted();
        } catch (Exception err) {
            // expected
        }

        // Fetch a copy of the SharedId shared from the first client.
        final TableHandle client2Copy = client2.execute(sharedId.ticketId().table());

        // Close the first client's session.
        session.close();

        // Publish to the query scope via the second client's shared copy.
        client2.publish(destId, client2Copy).get();

        // Ensure that we can resolve from query scope.
        client2.execute(destId.ticketId().table());
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
    public void testSharePublishCompletesImmediately() throws Exception {
        final TicketId ticketId = new TicketId(ExportTicketHelper.exportIdToBytes(1));
        final SharedId sharedId = SharedId.newRandom();

        // Let's publish the source table to the shared id.
        session.publish(sharedId, ticketId).get();
    }

    @Test
    public void testScopePublishWaitsForSourceCompletion() throws Exception {
        final TicketId ticketId = new TicketId(ExportTicketHelper.exportIdToBytes(1));
        final ScopeId scopeId = new ScopeId("test_dest_table");

        // Let's publish the source table to the shared id.
        final CompletableFuture<Void> rpc = session.publish(scopeId, ticketId);
        try {
            rpc.get(250, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException ignored) {
            // This is expected.
        }

        final TimeTable sourceTable = TimeTable.of(Duration.ofSeconds(1));
        final TableHandle remoteSource = session.execute(sourceTable);

        // Now the RPC should complete.
        rpc.get(250, TimeUnit.MILLISECONDS);
    }
}
