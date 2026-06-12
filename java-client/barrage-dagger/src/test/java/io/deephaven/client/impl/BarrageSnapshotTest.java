//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;//

// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

import io.deephaven.engine.table.Table;

import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.qst.table.NewTable;
import io.deephaven.server.runner.DeephavenApiServerTestBase;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.util.TestDataUtil;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end Barrage snapshot tests that verify full round-trip serialization of all supported column types through the
 * DoExchange bidi streaming mechanism. The key differentiator of these tests is that they exercise the complete
 * {@link BarrageSnapshot#entireTable()} path including client-side table materialization.
 */
public class BarrageSnapshotTest extends DeephavenApiServerTestBase {

    private BufferAllocator bufferAllocator;
    private ScheduledExecutorService barrageScheduler;
    private BarrageSession barrageSession;
    private SessionState serverSessionState;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        bufferAllocator = new RootAllocator();
        barrageScheduler = Executors.newScheduledThreadPool(4);

        final ManagedChannel managedChannel = channelBuilder().build();
        register(managedChannel);

        final var barrage = DaggerDeephavenBarrageRoot.create()
                .factoryBuilder()
                .managedChannel(managedChannel)
                .allocator(bufferAllocator)
                .scheduler(barrageScheduler)
                .build();

        barrageSession = barrage.newBarrageSession();
        serverSessionState = server().sessionService().getSessionForToken(
                ((io.deephaven.client.impl.SessionImpl) barrageSession.session())._hackBearerHandler()
                        .getCurrentToken());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (barrageSession != null) {
            barrageSession.close();
        }
        if (barrageScheduler != null) {
            barrageScheduler.shutdownNow();
            try {
                if (!barrageScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    throw new RuntimeException("Barrage scheduler not shutdown after 5 seconds");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (bufferAllocator != null) {
            bufferAllocator.close();
        }
        super.tearDown();
    }

    /**
     * Comprehensive test covering all supported column types including nulls. This exercises the full Barrage snapshot
     * round-trip via DoExchange bidi streaming.
     */
    @Test(timeout = 10000)
    public void snapshotAllTypes() throws Exception {
        final Table sourceTable = InMemoryTable.from(TestDataUtil.getTestDataNewTable());

        final ExportObject<Table> export = serverSessionState.newServerSideExport(sourceTable);
        final io.deephaven.qst.table.TableSpec tableSpec = io.deephaven.qst.table.TableSpec
                .ticket(export.getExportId().getTicket().toByteArray());

        try (final TableHandle handle = barrageSession.session().serial().execute(tableSpec)) {
            assertThat(handle.isSuccessful()).isTrue();

            final BarrageSnapshot snapshot = barrageSession
                    .snapshot(handle, BarrageSnapshotOptions.builder().build());

            final Table result = snapshot.entireTable().get();
            assertThat(result.size()).isEqualTo(3);
            TstUtils.assertTableEquals(sourceTable, result);
        }
    }

    /**
     * This test is similar to {@link #snapshotAllTypes()}, but it uses Flight instead of Barrage to publish the table
     * to the server, which covers {@link FieldVectorAdapter}. This verifies that a NewTable published via Flight (then
     * retrieved with Barrage) matches a NewTable converted locally to an InMemoryTable.
     */
    @Test(timeout = 10000)
    public void snapshotAllTypesFlight() throws Exception {
        final NewTable newTable = TestDataUtil.getTestDataNewTable();
        final Table inMemoryTable = InMemoryTable.from(newTable);

        try (final TableHandle newTableHandle = barrageSession.putExport(newTable, bufferAllocator)) {
            final BarrageSnapshot snapshot = barrageSession
                    .snapshot(newTableHandle, BarrageSnapshotOptions.builder().build());

            final Table result = snapshot.entireTable().get();
            assertThat(result.size()).isEqualTo(3);
            TstUtils.assertTableEquals(inMemoryTable, result);
        }
    }

}
