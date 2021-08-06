package io.deephaven.client.examples;

import io.deephaven.client.impl.Export;
import io.deephaven.client.impl.Flight;
import io.deephaven.client.impl.SessionAndFlight;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightStream;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Command(name = "get-tsv", mixinStandardHelpOptions = true,
    description = "Send a QST, get the results, and convert to a TSV", version = "0.1.0")
class GetTsv extends FlightExampleBase {

    @Parameters(arity = "1", paramLabel = "QST", description = "QST file to send and get.",
        converter = TableConverter.class)
    TableSpec table;

    @Override
    protected void execute(SessionAndFlight sessionAndFlight) throws Exception {
        final long start = System.nanoTime();
        final long end;
        try (final Flight flight = sessionAndFlight.flight();
            final Export export = sessionAndFlight.session().export(table);
            final FlightStream stream = flight.get(export)) {
            stream.next();
            stream.getRoot();
            end = System.nanoTime();

            System.out.println(stream.getSchema());
            System.out.println();
            System.out.println(stream.getRoot().contentToTSVString());
        }
        System.out.printf("%s duration%n", Duration.ofNanos(end - start));
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetTsv()).execute(args);
        System.exit(execute);
    }
}
