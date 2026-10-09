//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageSnapshotOptions;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.proto.util.ScopeTicketHelper;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Barrage wire format over a raw Arrow Flight DoExchange, built by hand the way the {@code do-exchange} example
 * does it: the {@code dphn} magic in the descriptor, a {@code BarrageMessageWrapper} around a
 * {@code BarrageSnapshotRequest}. The barrage tests cover the same path through {@code BarrageSession}; this pins the
 * framing itself, so a server-side change that the Java wrapper happened to track still shows up.
 */
class DoExchangeTest {

    /** ASCII "dphn", the magic the server expects in the descriptor command and in the wrapper. */
    private static final byte[] MAGIC_BYTES = {100, 112, 104, 110};
    private static final int MAGIC = 0x6E687064;

    private static BufferAllocator allocator;
    private static ScheduledExecutorService scheduler;
    private static FlightSessionFactoryConfig.Factory factory;

    @BeforeAll
    static void connect() {
        allocator = new RootAllocator();
        scheduler = Executors.newScheduledThreadPool(4);
        factory = FlightSessionFactoryConfig.builder()
                .clientConfig(TestServer.clientConfig())
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
    }

    @AfterAll
    static void disconnect() throws InterruptedException {
        factory.managedChannel().shutdownNow();
        scheduler.shutdownNow();
        allocator.close();
        NettyLeakRecorder.assertNoLeaks();
    }

    @Test
    void handBuiltSnapshotRequestReturnsTheTable() throws Exception {
        final String variable = "api_do_exchange";
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle handle = flight.session().execute(TableSpec.empty(7).view("I=ii"))) {
            flight.session().publish(variable, handle).get(10, TimeUnit.SECONDS);

            try (final FlightClient.ExchangeReaderWriter erw =
                    flight.startExchange(FlightDescriptor.command(MAGIC_BYTES))) {
                // The inner message: a snapshot of the scope variable, all rows and columns, server-side defaults
                final FlatBufferBuilder inner = new FlatBufferBuilder();
                final int options = BarrageSnapshotOptions.createBarrageSnapshotOptions(inner, false, 0, 0, 0);
                final int ticket = BarrageSnapshotRequest.createTicketVector(inner,
                        ScopeTicketHelper.nameToBytes(variable));
                BarrageSnapshotRequest.startBarrageSnapshotRequest(inner);
                BarrageSnapshotRequest.addColumns(inner, 0);
                BarrageSnapshotRequest.addViewport(inner, 0);
                BarrageSnapshotRequest.addSnapshotOptions(inner, options);
                BarrageSnapshotRequest.addTicket(inner, ticket);
                inner.finish(BarrageSnapshotRequest.endBarrageSnapshotRequest(inner));

                // The outer wrapper: the magic, the message type, and the inner bytes
                final FlatBufferBuilder wrapper = new FlatBufferBuilder();
                final int payload = wrapper.createByteVector(inner.dataBuffer());
                wrapper.finish(BarrageMessageWrapper.createBarrageMessageWrapper(
                        wrapper, MAGIC, BarrageMessageType.BarrageSnapshotRequest, payload));

                final byte[] bytes = wrapper.sizedByteArray();
                // putMetadata takes ownership of the buffer and releases it; closing it here would double-release
                final ArrowBuf metadata = allocator.buffer(bytes.length);
                metadata.writeBytes(bytes);
                erw.getWriter().putMetadata(metadata);
                // A snapshot request has nothing more to say from the client side
                erw.getWriter().completed();

                final List<Long> values = new ArrayList<>();
                while (erw.getReader().next()) {
                    final BigIntVector i = (BigIntVector) erw.getReader().getRoot().getVector("I");
                    for (int r = 0; r < erw.getReader().getRoot().getRowCount(); ++r) {
                        values.add(i.get(r));
                    }
                }
                assertThat(values).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L);
            }
        }
    }
}
