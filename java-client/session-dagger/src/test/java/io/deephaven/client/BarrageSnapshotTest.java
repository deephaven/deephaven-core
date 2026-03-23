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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.deephaven.engine.util.TableTools.col;
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

    @Test(timeout = 10000)
    public void snapshotAllTypes() throws Exception {
        // Comprehensive test covering all supported column types including nulls.
        // This exercises the full Barrage snapshot round-trip via DoExchange bidi streaming.
        final Table sourceTable = TableTools.newTable(
                col("booleanCol", true, false, true),
                col("byteCol", (byte) 1, (byte) 2, (byte) 3),
                col("charCol", 'a', 'b', 'c'),
                col("shortCol", (short) 10, (short) 20, (short) 30),
                col("intCol", 100, 200, 300),
                col("longCol", 1000L, 3000000000L, 3000L),
                col("floatCol", 1.1f, 2.2f, 3.3f),
                col("doubleCol", 1.1, 2.2, 3.3),
                col("stringCol", "hello", null, "world"),
                col("instantCol",
                        Instant.parse("2025-01-01T10:00:00Z"),
                        null,
                        Instant.parse("2025-01-03T12:00:00Z")),
                col("localTimeCol",
                        LocalTime.of(10, 30, 45),
                        null,
                        LocalTime.of(22, 45, 0)),
                col("localDateCol",
                        LocalDate.of(2025, 1, 1),
                        null,
                        LocalDate.of(2025, 12, 31)),
                col("durationCol",
                        Duration.ofHours(1),
                        null,
                        Duration.ofMinutes(30)),
                col("periodCol",
                        Period.ofDays(1),
                        null,
                        Period.ofMonths(2)),
                col("bigIntegerCol",
                        new BigInteger("12345678901234567890"),
                        null,
                        BigInteger.valueOf(987654321L)),
                col("bigDecimalCol",
                        new BigDecimal("12345678901234567890.123456789"),
                        null,
                        new BigDecimal("789.012")),
                col("booleanArrayCol",
                        new Boolean[] {true, false},
                        null,
                        new Boolean[] {true}),
                col("byteArrayCol",
                        new byte[] {1, 2},
                        null,
                        new byte[] {3}),
                col("charArrayCol",
                        new char[] {'a', 'b'},
                        null,
                        new char[] {'c'}),
                col("shortArrayCol",
                        new short[] {10, 20},
                        null,
                        new short[] {30}),
                col("intArrayCol",
                        new int[] {100, 200},
                        null,
                        new int[] {300}),
                col("longArrayCol",
                        new long[] {1000L, 3000000000L},
                        null,
                        new long[] {3000L}),
                col("floatArrayCol",
                        new float[] {1.1f, 2.2f},
                        null,
                        new float[] {3.3f}),
                col("doubleArrayCol",
                        new double[] {1.1, 2.2},
                        null,
                        new double[] {3.3}),
                col("stringArrayCol",
                        new String[] {"a", "b"},
                        null,
                        new String[] {"c"}),
                col("instantArrayCol",
                        new Instant[] {Instant.parse("2025-01-01T10:00:00Z")},
                        null,
                        new Instant[] {Instant.parse("2025-01-03T12:00:00Z")}),
                col("localTimeArrayCol",
                        new LocalTime[] {LocalTime.of(9, 0)},
                        null,
                        new LocalTime[] {LocalTime.of(18, 0)}),
                col("localDateArrayCol",
                        new LocalDate[] {LocalDate.of(2025, 1, 15)},
                        null,
                        new LocalDate[] {LocalDate.of(2025, 12, 25)}),
                col("durationArrayCol",
                        new Duration[] {Duration.ofHours(2)},
                        null,
                        new Duration[] {Duration.ofSeconds(45)}),
                col("periodArrayCol",
                        new Period[] {Period.ofWeeks(1)},
                        null,
                        // Note: a period of 1 year would be converted to 12 months during serialization;
                        // Arrow does not have a type for years/months/days and java.time.Period stores years and months
                        // separately.
                        // See org.apache.arrow.vector.types.IntervalUnit.
                        new Period[] {Period.ofMonths(12)}),
                col("bigIntegerArrayCol",
                        new BigInteger[] {new BigInteger("12345678901234567890")},
                        null,
                        new BigInteger[] {BigInteger.valueOf(456L)}),
                col("bigDecimalArrayCol",
                        new BigDecimal[] {new BigDecimal("12345678901234567890.123456789")},
                        null,
                        new BigDecimal[] {new BigDecimal("4.56")}));

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
}
