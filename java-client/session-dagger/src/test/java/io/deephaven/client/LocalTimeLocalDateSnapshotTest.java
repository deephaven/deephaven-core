//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSnapshot;
import io.deephaven.client.impl.DaggerDeephavenBarrageRoot;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.server.runner.DeephavenApiServerTestBase;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.deephaven.engine.util.TableTools.col;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for LocalTime and LocalDate types through Arrow Flight (DoGet). Verifies that LocalTime and LocalDate
 * columns are correctly serialized via Arrow schema and data can be read back through the gRPC Flight service.
 */
public class LocalTimeLocalDateSnapshotTest extends DeephavenApiServerTestBase {

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

    @Test(timeout = 5000)
    public void canCreateTablesWithLocalTimeAndLocalDate() throws Exception {
        // Simple test to verify table creation works
        final Table sourceTable = TableTools.newTable(
                col("localTimeCol",
                        LocalTime.of(10, 30, 45),
                        LocalTime.of(14, 15, 30)),
                col("localDateCol",
                        LocalDate.of(2025, 11, 13),
                        LocalDate.of(2025, 11, 14)));

        assertThat(sourceTable).isNotNull();
        assertThat(sourceTable.size()).isEqualTo(2);
    }

    @Test(timeout = 5000)
    public void canCreateAndExportTableWithLocalTimeAndLocalDate() throws Exception {
        // Test that we can create and export a table
        final Table sourceTable = TableTools.newTable(
                col("localTimeCol",
                        LocalTime.of(10, 30, 45),
                        LocalTime.of(14, 15, 30)),
                col("localDateCol",
                        LocalDate.of(2025, 11, 13),
                        LocalDate.of(2025, 11, 14)));

        assertThat(sourceTable).isNotNull();
        assertThat(sourceTable.size()).isEqualTo(2);

        // Verify we can export it
        final ExportObject<Table> export = serverSessionState.newServerSideExport(sourceTable);
        assertThat(export).isNotNull();
        assertThat(export.getExportId()).isNotNull();
    }

    @Test(timeout = 10000)
    public void barrageSnapshotLocalTimeAndLocalDate() throws Exception {
        final Table sourceTable = TableTools.newTable(
                col("localTimeCol",
                        LocalTime.of(10, 30, 45),
                        LocalTime.of(14, 15, 30),
                        LocalTime.of(22, 45, 0)),
                col("localDateCol",
                        LocalDate.of(2025, 11, 13),
                        LocalDate.of(2025, 11, 14),
                        LocalDate.of(2025, 11, 15)));

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

    @Test(timeout = 10000)
    public void flightStreamLocalTimeAndLocalDate() throws Exception {
        final Table sourceTable = TableTools.newTable(
                col("localTimeCol",
                        LocalTime.of(10, 30, 45),
                        LocalTime.of(14, 15, 30),
                        LocalTime.of(22, 45, 0)),
                col("localDateCol",
                        LocalDate.of(2025, 11, 13),
                        LocalDate.of(2025, 11, 14),
                        LocalDate.of(2025, 11, 15)));

        final ExportObject<Table> export = serverSessionState.newServerSideExport(sourceTable);
        final io.deephaven.qst.table.TableSpec tableSpec = io.deephaven.qst.table.TableSpec
                .ticket(export.getExportId().getTicket().toByteArray());

        try (final TableHandle handle = barrageSession.session().serial().execute(tableSpec)) {
            assertThat(handle.isSuccessful()).isTrue();

            // Use DoGet (server-streaming) to read the data back via Arrow Flight
            try (final FlightStream stream = barrageSession.stream(handle)) {
                final Schema schema = stream.getSchema();
                // Verify schema has correct Arrow types
                assertThat(schema.findField("localTimeCol").getType())
                        .isInstanceOf(ArrowType.Time.class);
                assertThat(schema.findField("localDateCol").getType())
                        .isInstanceOf(ArrowType.Date.class);

                int totalRows = 0;
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
                assertThat(totalRows).isEqualTo(3);
            }
        }
    }

    @Test(timeout = 10000)
    public void flightStreamLocalTimeAndLocalDateWithNulls() throws Exception {
        final Table sourceTable = TableTools.newTable(
                col("localTimeWithNull",
                        LocalTime.of(10, 30),
                        null,
                        LocalTime.of(20, 45)),
                col("localDateWithNull",
                        LocalDate.of(2025, 1, 1),
                        null,
                        LocalDate.of(2025, 12, 31)));

        final ExportObject<Table> export = serverSessionState.newServerSideExport(sourceTable);
        final io.deephaven.qst.table.TableSpec tableSpec = io.deephaven.qst.table.TableSpec
                .ticket(export.getExportId().getTicket().toByteArray());

        try (final TableHandle handle = barrageSession.session().serial().execute(tableSpec)) {
            assertThat(handle.isSuccessful()).isTrue();

            try (final FlightStream stream = barrageSession.stream(handle)) {
                final Schema schema = stream.getSchema();
                assertThat(schema.findField("localTimeWithNull").getType())
                        .isInstanceOf(ArrowType.Time.class);
                assertThat(schema.findField("localDateWithNull").getType())
                        .isInstanceOf(ArrowType.Date.class);

                int totalRows = 0;
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
                assertThat(totalRows).isEqualTo(3);
            }
        }
    }

    @Test(timeout = 10000)
    public void flightStreamAllTypesIncludingLocalTimeAndLocalDate() throws Exception {
        final Table sourceTable = TableTools.newTable(
                col("byteCol", (byte) 1, (byte) 2, (byte) 3),
                col("intCol", 1000, 2000, 3000),
                col("longCol", 100000L, 200000L, 300000L),
                col("doubleCol", 1.1, 2.2, 3.3),
                col("stringCol", "hello", "world", "test"),
                col("localTimeCol",
                        LocalTime.of(10, 30, 45),
                        LocalTime.of(14, 15, 30),
                        LocalTime.of(22, 45, 0)),
                col("localDateCol",
                        LocalDate.of(2025, 11, 13),
                        LocalDate.of(2025, 11, 14),
                        LocalDate.of(2025, 11, 15)));

        final ExportObject<Table> export = serverSessionState.newServerSideExport(sourceTable);
        final io.deephaven.qst.table.TableSpec tableSpec = io.deephaven.qst.table.TableSpec
                .ticket(export.getExportId().getTicket().toByteArray());

        try (final TableHandle handle = barrageSession.session().serial().execute(tableSpec)) {
            assertThat(handle.isSuccessful()).isTrue();

            try (final FlightStream stream = barrageSession.stream(handle)) {
                final Schema schema = stream.getSchema();
                // Verify all expected columns exist with correct types
                assertThat(schema.getFields()).hasSize(7);
                assertThat(schema.findField("localTimeCol").getType())
                        .isInstanceOf(ArrowType.Time.class);
                assertThat(schema.findField("localDateCol").getType())
                        .isInstanceOf(ArrowType.Date.class);

                int totalRows = 0;
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
                assertThat(totalRows).isEqualTo(3);
            }
        }
    }
}
