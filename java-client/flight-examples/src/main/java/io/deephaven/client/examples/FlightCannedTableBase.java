package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.qst.TableCreationLogic;
import org.apache.arrow.flight.FlightStream;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.time.Duration;

abstract class FlightCannedTableBase extends FlightExampleBase {

    static class Mode {
        @Option(names = {"-b", "--batch"}, required = true, description = "Batch mode")
        boolean batch;

        @Option(names = {"-s", "--serial"}, required = true, description = "Serial mode")
        boolean serial;
    }

    @ArgGroup(exclusive = true)
    Mode mode;

    protected abstract TableCreationLogic logic();

    @Override
    protected void execute(FlightSession flight) throws Exception {

        final TableHandleManager manager = mode == null ? flight.session()
                : mode.batch ? flight.session().batch() : flight.session().serial();

        final long start = System.nanoTime();
        final long end;
        try (final TableHandle handle = manager.executeLogic(logic());
                final FlightStream stream = flight.getStream(handle.export())) {
            System.out.println(stream.getSchema());
            while (stream.next()) {
                System.out.println(stream.getRoot().contentToTSVString());
            }
            end = System.nanoTime();

        }
        System.out.printf("%s duration%n", Duration.ofNanos(end - start));
    }
}
