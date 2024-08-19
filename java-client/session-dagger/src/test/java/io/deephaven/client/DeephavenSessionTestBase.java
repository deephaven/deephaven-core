//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.base.verify.Require;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.engine.table.Table;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.server.runner.DeephavenApiServerTestBase;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.ManagedChannel;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class DeephavenSessionTestBase extends DeephavenApiServerTestBase {

    protected ScheduledExecutorService sessionScheduler;
    protected Session session;
    protected SessionState serverSessionState;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ManagedChannel channel = channelBuilder().build();
        register(channel);
        sessionScheduler = Executors.newScheduledThreadPool(2);
        final SessionImpl clientSessionImpl = DaggerDeephavenSessionRoot.create()
                .factoryBuilder()
                .managedChannel(channel)
                .scheduler(sessionScheduler)
                .build()
                .newSession();
        session = clientSessionImpl;
        serverSessionState = Require.neqNull(server().sessionService().getSessionForToken(
                clientSessionImpl._hackBearerHandler().getCurrentToken()), "SessionState");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        session.close();
        sessionScheduler.shutdownNow();
        if (!sessionScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
            throw new RuntimeException("Scheduler not shutdown within 5 seconds");
        }
        super.tearDown();
    }

    public TicketTable ref(Table table) {
        final ExportObject<Table> export = serverSessionState.newServerSideExport(table);
        return TableSpec.ticket(export.getExportId().getTicket().toByteArray());
    }
}
