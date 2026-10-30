//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.impl.ChannelHelper;
import io.deephaven.client.impl.ClientConfig;
import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.grpc.compression.CompressionCodecs;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders2;
import io.deephaven.qst.table.NewTable;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientStreamTracer;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * gRPC message compression against the server: DoGet responses compressed with each built-in type a table allows, and
 * DoPut uploads compressed with each built-in type a client's {@link ClientConfig#requestCompression()} names. A
 * recording interceptor reads the {@code grpc-encoding} of each, so these tests fail if the data goes uncompressed.
 */
class CompressionTest {

    private static final int ROWS = 10_000;

    private static final Metadata.Key<String> GRPC_ENCODING =
            Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);

    private static BufferAllocator allocator;
    private static ScheduledExecutorService scheduler;

    @BeforeAll
    static void setUp() {
        // both tests loop over the supported types, and would pass without testing anything if there were none
        assertThat(CompressionCodecs.SUPPORTED).as("no compression types on the classpath to test").isNotEmpty();
        allocator = new RootAllocator();
        scheduler = Executors.newScheduledThreadPool(4);
    }

    @AfterAll
    static void tearDown() {
        scheduler.shutdownNow();
        // Throws if any Arrow buffer the tests read was not released
        allocator.close();
    }

    @Test
    void responsesUseTheTypeEachTableAllows() throws Exception {
        final EncodingRecorder recorder = new EncodingRecorder();
        final FlightSessionFactoryConfig.Factory factory = factory(TestServer.clientConfig(), recorder);
        try (
                final FlightSession flight = factory.newFlightSession();
                final ConsoleSession console = flight.session().console("python").get(10, TimeUnit.SECONDS)) {
            final List<String> script = new ArrayList<>(List.of("from deephaven import empty_table"));
            for (final String compression : CompressionCodecs.SUPPORTED) {
                script.add(tableName(compression) + " = empty_table(" + ROWS
                        + ").update(['I = ii', 'S = `sym` + (ii % 32)']).with_transport_compression('" + compression
                        + "')");
            }
            console.executeCode(String.join("\n", script));

            for (final String compression : CompressionCodecs.SUPPORTED) {
                recorder.clear();
                assertThat(rowCount(flight, new ScopeId(tableName(compression)))).isEqualTo(ROWS);
                // the client accepts every supported type by default
                assertThat(recorder.responseEncodings).as(compression).containsExactly(compression);
            }
        } finally {
            factory.managedChannel().shutdownNow();
        }
    }

    @Test
    void uploadsUseTheRequestCompression() throws Exception {
        final ColumnHeaders2<Integer, String>.Rows rows =
                ColumnHeader.ofInt("I").header(ColumnHeader.ofString("S")).start(ROWS);
        for (int ii = 0; ii < ROWS; ++ii) {
            rows.row(ii, "sym" + (ii % 32));
        }
        final NewTable table = rows.newTable();

        for (final String compression : CompressionCodecs.SUPPORTED) {
            final EncodingRecorder recorder = new EncodingRecorder();
            final ClientConfig config = ClientConfig.builder()
                    .target(TestServer.target())
                    .requestCompression(compression)
                    .build();
            final FlightSessionFactoryConfig.Factory factory = factory(config, recorder);
            try (
                    final FlightSession flight = factory.newFlightSession();
                    final TableHandle handle = flight.putExport(table, allocator)) {
                assertThat(handle.response().getSize()).as(compression).isEqualTo(ROWS);
                assertThat(rowCount(flight, handle)).as(compression).isEqualTo(ROWS);
            } finally {
                factory.managedChannel().shutdownNow();
            }
            assertThat(recorder.requestEncodings).as(compression).containsExactly(compression);
            assertThat(recorder.wireBytes.get()).as(compression).isLessThan(recorder.uncompressedBytes.get());
        }
    }

    private static String tableName(final String compression) {
        return "api_compression_" + compression;
    }

    private static FlightSessionFactoryConfig.Factory factory(final ClientConfig config,
            final ClientInterceptor recorder) {
        return FlightSessionFactoryConfig.builder()
                .clientConfig(config)
                .clientChannelFactory(c -> ChannelHelper.channelBuilder(c).intercept(recorder).build())
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
    }

    private static long rowCount(final FlightSession flight, final HasTicketId ticket) throws Exception {
        long rows = 0;
        try (final FlightStream stream = flight.stream(ticket)) {
            while (stream.next()) {
                rows += stream.getRoot().getRowCount();
            }
        }
        return rows;
    }

    /**
     * Records the {@code grpc-encoding} of each DoGet response, and of each DoPut request with its uncompressed and
     * on-the-wire sizes.
     */
    private static final class EncodingRecorder implements ClientInterceptor {
        private final List<String> responseEncodings = Collections.synchronizedList(new ArrayList<>());
        private final List<String> requestEncodings = Collections.synchronizedList(new ArrayList<>());
        private final AtomicLong uncompressedBytes = new AtomicLong();
        private final AtomicLong wireBytes = new AtomicLong();

        void clear() {
            responseEncodings.clear();
            requestEncodings.clear();
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                final MethodDescriptor<ReqT, RespT> method,
                final CallOptions callOptions,
                final Channel next) {
            switch (method.getBareMethodName()) {
                case "DoGet":
                    return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                        @Override
                        public void start(final Listener<RespT> responseListener, final Metadata headers) {
                            super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(
                                    responseListener) {
                                @Override
                                public void onHeaders(final Metadata responseHeaders) {
                                    final String encoding = responseHeaders.get(GRPC_ENCODING);
                                    responseEncodings.add(encoding == null ? "identity" : encoding);
                                    super.onHeaders(responseHeaders);
                                }
                            }, headers);
                        }
                    };
                case "DoPut":
                    // the tracer sees the request headers after gRPC adds grpc-encoding, and every message's size
                    return next.newCall(method, callOptions.withStreamTracerFactory(new ClientStreamTracer.Factory() {
                        @Override
                        public ClientStreamTracer newClientStreamTracer(
                                final ClientStreamTracer.StreamInfo info,
                                final Metadata headers) {
                            final String encoding = headers.get(GRPC_ENCODING);
                            requestEncodings.add(encoding == null ? "identity" : encoding);
                            return new ClientStreamTracer() {
                                @Override
                                public void outboundUncompressedSize(final long bytes) {
                                    uncompressedBytes.addAndGet(bytes);
                                }

                                @Override
                                public void outboundWireSize(final long bytes) {
                                    wireBytes.addAndGet(bytes);
                                }
                            };
                        }
                    }));
                default:
                    return next.newCall(method, callOptions);
            }
        }
    }
}
