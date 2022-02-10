package io.deephaven.client.examples;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import io.deephaven.client.impl.FlightSession;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.nio.ByteBuffer;

@Command(name = "do-exchange", mixinStandardHelpOptions = true,
        description = "Start a DoExchange session with the server", version = "0.1.0")
class DoExchange extends FlightExampleBase {

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Override
    protected void execute(FlightSession flight) throws Exception {


        ///////////////////////////////////////////////
        // Passing case
        ///////////////////////////////////////////////

        // create a BarrageMessage byte array equating to BarrageMessage.NONE

        byte[] cmd = new byte[] {12, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 100, 112, 104, 110};

        FlightDescriptor fd = FlightDescriptor.command(cmd);

        try (FlightClient.ExchangeReaderWriter erw = flight.startExchange(fd);
                final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {

            // BarrageSnapshotRequest for ticket 's/timetable'
            cmd = new byte[] {16, 0, 0, 0, 0, 0, 10, 0, 16, 0, 8, 0, 7, 0, 12, 0, 10, 0, 0, 0, 0, 0, 0, 7, 100, 112,
                    104, 110, 4, 0, 0, 0, 52, 0, 0, 0, 16, 0, 0, 0, 12, 0, 12, 0, 4, 0, 0, 0, 0, 0, 8, 0, 12, 0, 0, 0,
                    8, 0, 0, 0, 24, 0, 0, 0, 11, 0, 0, 0, 115, 47, 116, 105, 109, 101, 116, 97, 98, 108, 101, 0, 4, 0,
                    4, 0, 4, 0, 0, 0};

            ArrowBuf data = allocator.buffer(cmd.length);
            data.writeBytes(cmd);
            erw.getWriter().putMetadata(data);
            erw.getWriter().completed();

            // read everything from the server
            while (erw.getReader().next()) {
                // NOP
            }

            System.out.println(erw.getReader().getSchema().toString());
            System.out.println(erw.getReader().getRoot().contentToTSVString());

            Thread.sleep(2000);

        }

        ///////////////////////////////////////////////
        // Failing case 1 - empty FlightDescriptor
        ///////////////////////////////////////////////

        cmd = new byte[0];
        fd = FlightDescriptor.command(cmd);
        try (FlightClient.ExchangeReaderWriter erw = flight.startExchange(fd);
                final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {

            cmd = new byte[0];

            ArrowBuf data = allocator.buffer(cmd.length);
            data.writeBytes(cmd);
            erw.getWriter().putMetadata(data);

            erw.getWriter().completed();

            Thread.sleep(2000);
        }

        ///////////////////////////////////////////////
        // Failing case 2 - valid FlightDescriptor, empty metadata
        ///////////////////////////////////////////////

        cmd = new byte[] {12, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 100, 112, 104, 110};
        fd = FlightDescriptor.command(cmd);
        try (FlightClient.ExchangeReaderWriter erw = flight.startExchange(fd);
             final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {

            cmd = new byte[0];

            ArrowBuf data = allocator.buffer(cmd.length);
            data.writeBytes(cmd);
            erw.getWriter().putMetadata(data);

            erw.getWriter().completed();

            Thread.sleep(2000);
        }

    }

    public static void main(String[] args) {
        int execute = new CommandLine(new DoExchange()).execute(args);
        System.exit(execute);
    }
}
