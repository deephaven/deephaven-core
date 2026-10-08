//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageSnapshotOptions;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.proto.util.ScopeTicketHelper;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Requests a Barrage snapshot of a scope table over a raw Arrow Flight DoExchange, building the flatbuffer request by
 * hand. The barrage examples show the same thing through the client's {@code BarrageSession} wrapper.
 */
@Command(name = "do-exchange", mixinStandardHelpOptions = true,
        description = "Start a DoExchange session with the server", version = "0.1.0")
class DoExchange implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Override
    public Void call() throws Exception {
        // Arrow memory for the data read and written over Flight
        final BufferAllocator allocator = new RootAllocator();
        // The scheduler runs the client's background work, such as refreshing the session token
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        // The factory holds the connection; each session it opens is one authenticated login on that connection
        final FlightSessionFactoryConfig.Factory factory = FlightSessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
        // A FlightSession pairs a Session (tables, consoles, publishing) with an Arrow Flight client (bulk data)
        try (final FlightSession flight = factory.newFlightSession()) {
            // need to provide the MAGIC bytes as the FlightDescriptor.cmd in the initial message
            byte[] cmd = new byte[] {100, 112, 104, 110}; // equivalent to '0x6E687064' (ASCII "dphn")

            final FlightDescriptor fd = FlightDescriptor.command(cmd);

            // create the bi-directional reader/writer
            try (final FlightClient.ExchangeReaderWriter erw = flight.startExchange(fd)) {

                /////////////////////////////////////////////////////////////
                // create a BarrageSnapshotRequest for the ticket
                /////////////////////////////////////////////////////////////

                // inner metadata for the snapshot request
                final FlatBufferBuilder metadata = new FlatBufferBuilder();

                // you can use 0 for batch size and max message size to use server-side defaults
                final int optOffset = BarrageSnapshotOptions.createBarrageSnapshotOptions(metadata, false, 0, 0, 0);

                final int ticOffset =
                        BarrageSnapshotRequest.createTicketVector(metadata,
                                ScopeTicketHelper.nameToBytes(ticket.scopeField.variable));
                BarrageSnapshotRequest.startBarrageSnapshotRequest(metadata);
                BarrageSnapshotRequest.addColumns(metadata, 0);
                BarrageSnapshotRequest.addViewport(metadata, 0);
                BarrageSnapshotRequest.addSnapshotOptions(metadata, optOffset);
                BarrageSnapshotRequest.addTicket(metadata, ticOffset);
                metadata.finish(BarrageSnapshotRequest.endBarrageSnapshotRequest(metadata));

                // outer metadata to ID the message type and provide the MAGIC bytes
                final FlatBufferBuilder wrapper = new FlatBufferBuilder();
                final int innerOffset = wrapper.createByteVector(metadata.dataBuffer());
                wrapper.finish(BarrageMessageWrapper.createBarrageMessageWrapper(
                        wrapper,
                        0x6E687064, // the numerical representation of the ASCII "dphn".
                        BarrageMessageType.BarrageSnapshotRequest,
                        innerOffset));

                // extract the bytes and package them in an ArrowBuf for transmission
                cmd = wrapper.sizedByteArray();
                final ArrowBuf data = allocator.buffer(cmd.length);
                data.writeBytes(cmd);

                // `putMetadata()` makes the GRPC call
                erw.getWriter().putMetadata(data);

                // snapshot requests do not need to stay open on the client side
                erw.getWriter().completed();

                // read everything from the server
                while (erw.getReader().next()) {
                    // NOP
                }

                // print the table data
                System.out.println(erw.getReader().getSchema().toString());
                System.out.println(erw.getReader().getRoot().contentToTSVString());
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new DoExchange()).execute(args));
    }
}
