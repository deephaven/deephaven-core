package io.deephaven.client.examples;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.proto.util.ScopeTicketHelper;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import io.deephaven.barrage.flatbuf.*;

@Command(name = "do-exchange", mixinStandardHelpOptions = true,
        description = "Start a DoExchange session with the server", version = "0.1.0")
class DoExchange extends FlightExampleBase {

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Override
    protected void execute(FlightSession flight) throws Exception {

        // need to provide the MAGIC bytes as the FlightDescriptor.cmd in the initial message
        byte[] cmd = new byte[] {100, 112, 104, 110}; // equivalent to '0x6E687064' (ASCII "dphn")

        FlightDescriptor fd = FlightDescriptor.command(cmd);

        // create the bi-directional reader/writer
        try (FlightClient.ExchangeReaderWriter erw = flight.startExchange(fd);
                final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {

            /////////////////////////////////////////////////////////////
            // create a BarrageSnapshotRequest for ticket 's/timetable'
            /////////////////////////////////////////////////////////////

            // inner metadata for the snapshot request
            final FlatBufferBuilder metadata = new FlatBufferBuilder();

            int optOffset =
                    BarrageSnapshotOptions.createBarrageSnapshotOptions(metadata, ColumnConversionMode.Stringify,
                            false, 1000);

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
            ArrowBuf data = allocator.buffer(cmd.length);
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
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new DoExchange()).execute(args);
        System.exit(execute);
    }
}
