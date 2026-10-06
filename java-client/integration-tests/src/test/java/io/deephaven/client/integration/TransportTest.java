//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.impl.ClientConfig;
import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionData;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.qst.table.TableSpec;
import io.netty.handler.ssl.OpenSsl;
import io.netty.util.Version;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the transport under the client rather than the client API: HTTP/2 flow control with large transfers, many
 * streams multiplexed over one channel, cancellation and deadlines leaving the channel usable, the native SSL provider
 * loading, and netty's buffer accounting. This is the test to run after bumping netty or gRPC.
 */
class TransportTest {

    /** About 80MB over the wire as two 8-byte columns, in many record batches. */
    private static final long LARGE_ROWS = 5_000_000;
    private static final TableSpec LARGE = TableSpec.empty(LARGE_ROWS).view("I=ii", "D=ii*0.5");

    private static final Duration TIMEOUT = Duration.ofMinutes(2);

    private static BufferAllocator allocator;
    private static ScheduledExecutorService scheduler;
    private static FlightSessionFactoryConfig.Factory factory;

    @BeforeAll
    static void connect() {
        allocator = new RootAllocator();
        scheduler = Executors.newScheduledThreadPool(8);
        factory = factory(TestServer.clientConfig());
    }

    @AfterAll
    static void disconnectAndCheckForLeaks() throws Exception {
        factory.managedChannel().shutdownNow();
        scheduler.shutdownNow();
        // Every Arrow buffer the tests read must have been released; close() throws otherwise
        assertThat(allocator.getAllocatedMemory()).as("Arrow memory still allocated").isZero();
        allocator.close();
        NettyLeakRecorder.assertNoLeaks();
    }

    private static FlightSessionFactoryConfig.Factory factory(ClientConfig clientConfig) {
        return FlightSessionFactoryConfig.builder()
                .clientConfig(clientConfig)
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
    }

    @Test
    void sslProviderMatchesTheClasspath() {
        // ChannelHelper lets gRPC pick the SSL provider: BoringSSL when netty-tcnative's native library for this
        // platform is on the classpath, the JDK's otherwise. The classifier-less netty-tcnative-boringssl-static that
        // the client's dependencies bring in carries no natives, so today this is the JDK provider. Either way, the
        // provider must match what the classpath provides: a bump that adds natives which then fail to load, or that
        // drops them, shows up here before it shows up as a slow or broken TLS handshake.
        final String os = System.getProperty("os.name").toLowerCase().contains("mac") ? "osx" : "linux";
        final String arch = System.getProperty("os.arch").contains("aarch64") ? "aarch_64" : "x86_64";
        final String nativeJar = "netty-tcnative-boringssl-static-" + ".*" + os + "-" + arch;
        final boolean nativesOnClasspath =
                Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
                        .map(p -> Paths.get(p).getFileName().toString())
                        .anyMatch(name -> name.matches(nativeJar + ".*\\.jar"));

        // Visible in the test report, for a bump to be checked against
        Version.identify().forEach((artifact, version) -> System.out.println(artifact + " " + version));
        System.out.println("tcnative natives on classpath: " + nativesOnClasspath);
        System.out.println("OpenSsl available: " + OpenSsl.isAvailable()
                + (OpenSsl.isAvailable() ? " " + OpenSsl.versionString()
                        : ", cause: " + OpenSsl.unavailabilityCause()));

        assertThat(OpenSsl.isAvailable())
                .as("BoringSSL loaded, given natives on the classpath = " + nativesOnClasspath)
                .isEqualTo(nativesOnClasspath);
    }

    @Test
    void largeDoGetArrivesCompleteAndInOrder() throws Exception {
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle handle = flight.session().execute(LARGE)) {
            final long start = System.nanoTime();
            long rows = 0;
            long sum = 0;
            int batches = 0;
            try (final FlightStream stream = flight.stream(handle)) {
                while (stream.next()) {
                    final BigIntVector i = (BigIntVector) stream.getRoot().getVector("I");
                    for (int r = 0; r < stream.getRoot().getRowCount(); ++r) {
                        // In order: each value is the row number so far
                        assertThat(i.get(r)).isEqualTo(rows);
                        sum += i.get(r);
                        ++rows;
                    }
                    ++batches;
                }
            }
            System.out.printf("DoGet %d rows in %d batches, %s%n", rows, batches,
                    Duration.ofNanos(System.nanoTime() - start));
            assertThat(rows).isEqualTo(LARGE_ROWS);
            assertThat(sum).isEqualTo(LARGE_ROWS * (LARGE_ROWS - 1) / 2);
            assertThat(batches).as("flow control is exercised across many batches").isGreaterThan(1);
        }
    }

    @Test
    void largeDoPutRoundTrips() throws Exception {
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle source = flight.session().execute(LARGE);
                final FlightStream doGet = flight.stream(source);
                final TableHandle copy = flight.putExport(doGet)) {
            assertThat(copy.response().getSize()).isEqualTo(LARGE_ROWS);
            // Read the copy back: the upload path must have kept every row, in order
            long rows = 0;
            long sum = 0;
            try (final FlightStream stream = flight.stream(copy)) {
                while (stream.next()) {
                    final BigIntVector i = (BigIntVector) stream.getRoot().getVector("I");
                    for (int r = 0; r < stream.getRoot().getRowCount(); ++r) {
                        assertThat(i.get(r)).isEqualTo(rows);
                        sum += i.get(r);
                        ++rows;
                    }
                }
            }
            assertThat(rows).isEqualTo(LARGE_ROWS);
            assertThat(sum).isEqualTo(LARGE_ROWS * (LARGE_ROWS - 1) / 2);
        }
    }

    @Test
    void maxInboundMessageSizeIsEnforcedAndConfigurable() throws Exception {
        // A limit smaller than one record batch must fail the stream, and nothing else
        final ClientConfig small = ClientConfig.builder()
                .target(TestServer.target())
                .maxInboundMessageSize(64 * 1024)
                .build();
        final FlightSessionFactoryConfig.Factory smallFactory = factory(small);
        try (
                final FlightSession flight = smallFactory.newFlightSession();
                final TableHandle handle = flight.session().execute(LARGE)) {
            assertThatThrownBy(() -> {
                try (final FlightStream stream = flight.stream(handle)) {
                    while (stream.next()) {
                        // drain until the oversized batch arrives
                    }
                }
            }).hasStackTraceContaining("RESOURCE_EXHAUSTED");
        } finally {
            smallFactory.managedChannel().shutdownNow();
        }
        // The default limit is enough for the same table (largeDoGetArrivesCompleteAndInOrder proves the data)
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle handle = flight.session().execute(LARGE);
                final FlightStream stream = flight.stream(handle)) {
            assertThat(stream.next()).isTrue();
        }
    }

    @Test
    void manyStreamsMultiplexOverOneChannel() throws Exception {
        // Every call below is offered to the one connection at once: 32 DoGets, 300 unary calls, and a log stream,
        // which is deliberately more than the server allows concurrently (jetty's default is 128 streams), so the
        // client has to hold the rest in its pending queue and start each as an earlier stream closes
        final int readers = 32;
        final int unaryCalls = 300;
        final TableSpec medium = TableSpec.empty(200_000).view("I=ii");
        final ExecutorService pool = Executors.newCachedThreadPool();
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle handle = flight.session().execute(medium);
                final ConsoleSession console = flight.session().console("python").get(10, TimeUnit.SECONDS)) {
            // Two server-streaming calls stay open on the channel for the whole test
            final Iterator<LogSubscriptionData> logs = flight.session().channel().consoleBlocking()
                    .withDeadlineAfter(TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                    .subscribeToLogs(LogSubscriptionRequest.newBuilder().build());
            final Future<?> logReader = pool.submit(() -> {
                while (logs.hasNext()) {
                    logs.next();
                }
            });

            // Hold every worker until all are submitted, so they start their calls together rather than as the
            // submission loop reaches them
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<Long>> reads = new ArrayList<>();
            for (int i = 0; i < readers; ++i) {
                reads.add(pool.submit((Callable<Long>) () -> {
                    start.await();
                    long rows = 0;
                    try (final FlightStream stream = flight.stream(handle)) {
                        while (stream.next()) {
                            rows += stream.getRoot().getRowCount();
                        }
                    }
                    return rows;
                }));
            }
            final List<Future<Long>> unaries = new ArrayList<>();
            for (int i = 0; i < unaryCalls; ++i) {
                unaries.add(pool.submit((Callable<Long>) () -> {
                    start.await();
                    try (final TableHandle one = flight.session().execute(TableSpec.empty(1))) {
                        return one.response().getSize();
                    }
                }));
            }
            start.countDown();
            // And a console command in the middle of it, which the log subscription will see
            console.executeCode("print('multiplexing')");

            for (Future<Long> read : reads) {
                assertThat(read.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isEqualTo(200_000L);
            }
            for (Future<Long> unary : unaries) {
                assertThat(unary.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isEqualTo(1L);
            }
            assertThat(logReader.isDone()).as("log stream still open while the others completed").isFalse();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void cancelledStreamLeavesTheChannelUsable() throws Exception {
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle handle = flight.session().execute(LARGE)) {
            try (final FlightStream stream = flight.stream(handle)) {
                assertThat(stream.next()).isTrue();
                stream.cancel("cancelled by TransportTest", null);
            }
            // The same channel and session carry on
            try (
                    final TableHandle small = flight.session().execute(TableSpec.empty(3).view("I=ii"));
                    final FlightStream stream = flight.stream(small)) {
                long rows = 0;
                while (stream.next()) {
                    rows += stream.getRoot().getRowCount();
                }
                assertThat(rows).isEqualTo(3);
            }
        }
    }

    @Test
    void deadlineExceededLeavesTheChannelUsable() throws Exception {
        try (
                final FlightSession flight = factory.newFlightSession();
                final ConsoleSession console = flight.session().console("python").get(10, TimeUnit.SECONDS)) {
            final Session session = flight.session();
            final ExecuteCommandRequest slow = ExecuteCommandRequest.newBuilder()
                    .setConsoleId(console.ticket())
                    .setCode("import time\ntime.sleep(5)")
                    .build();
            assertThatThrownBy(() -> session.channel().consoleBlocking()
                    .withDeadlineAfter(1, TimeUnit.SECONDS)
                    .executeCommand(slow))
                    .hasMessageContaining("DEADLINE_EXCEEDED");
            // The deadline cancelled one stream; the channel is fine
            assertThat(session.getConfigurationConstants().get(10, TimeUnit.SECONDS)).isNotEmpty();
        }
    }
}
