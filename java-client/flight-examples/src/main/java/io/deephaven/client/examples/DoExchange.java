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


        try (final FlightStream stream = flight.stream(ticket)) {

            ///////////////////////////////////////////////
            // Passing case
            ///////////////////////////////////////////////

            // create a BarrageMessage byte array

            byte[] cmd = new byte[] {12, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 100, 112, 104, 110};

            FlightDescriptor fd = FlightDescriptor.command(cmd);

            try (FlightClient.ExchangeReaderWriter erw = flight.startExchange(fd);
                 final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {

                cmd = new byte[] {16, 0, 0, 0, 0, 0, 10, 0, 16, 0, 8, 0, 7, 0, 12, 0, 10, 0, 0, 0, 0, 0, 0, 7, 100, 112,
                        104, 110, 4, 0, 0, 0, 48, 0, 0, 0, 16, 0, 0, 0, 12, 0, 12, 0, 4, 0, 0, 0, 0, 0, 8, 0, 12, 0, 0,
                        0, 8, 0, 0, 0, 20, 0, 0, 0, 5, 0, 0, 0, 101, 1, 0, 0, 0, 0, 0, 0, 4, 0, 4, 0, 4, 0, 0, 0};

                ArrowBuf data = allocator.buffer(cmd.length);
                data.writeBytes(cmd);
                erw.getWriter().putMetadata(data);
                // erw.getWriter().putNext();

                // erw.getWriter().completed();
                Thread.sleep(1000000000);
            }

            ///////////////////////////////////////////////
            // Failing case 1 - null FlightDescriptor
            ///////////////////////////////////////////////

            // cmd = new byte[] { };
            // fd = FlightDescriptor.command(cmd);
            // try ( FlightClient.ExchangeReaderWriter erw = flight.startExchange(fd)) {
            //
            // // now pass in a Barrage
            //
            // erw.getWriter().completed();
            // }
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new DoExchange()).execute(args);
        System.exit(execute);
    }
}
