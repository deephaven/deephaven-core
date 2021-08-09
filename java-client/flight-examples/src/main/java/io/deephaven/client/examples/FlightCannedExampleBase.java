package io.deephaven.client.examples;

import io.deephaven.client.impl.Export;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightStream;

import java.time.Duration;

abstract class FlightCannedExampleBase extends FlightExampleBase {

    protected abstract TableSpec table();

    @Override
    protected void execute(FlightSession flight) throws Exception {
        TableSpec table = table();
        final long start = System.nanoTime();
        final long end;
        try (final Export export = flight.session().export(table);
            final FlightStream stream = flight.getStream(export)) {
            System.out.println(stream.getSchema());
            while (stream.next()) {
                System.out.println(stream.getRoot().contentToTSVString());
            }
            end = System.nanoTime();
        }
        System.out.printf("%s duration%n", Duration.ofNanos(end - start));
    }
}
