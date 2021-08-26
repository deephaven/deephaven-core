package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.qst.LabeledValue;
import io.deephaven.qst.TableCreationLabeledLogic;
import org.apache.arrow.flight.FlightStream;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.time.Duration;

abstract class FlightCannedLabeledTableBase extends FlightExampleBase {

    static class Mode {
        @Option(names = {"-b", "--batch"}, required = true, description = "Batch mode")
        boolean batch;

        @Option(names = {"-s", "--serial"}, required = true, description = "Serial mode")
        boolean serial;
    }

    @ArgGroup(exclusive = true)
    Mode mode;

    protected abstract TableCreationLabeledLogic logic();

    @Override
    protected void execute(FlightSession flight) throws Exception {
        final TableHandleManager manager = mode == null ? flight.session()
                : mode.batch ? flight.session().batch() : flight.session().serial();

        final long start = System.nanoTime();
        final long end;

        for (LabeledValue<TableHandle> handle : manager.executeLogic(logic())) {
            try (final FlightStream stream = flight.getStream(handle.value().export())) {
                System.out.println(handle.name());
                System.out.println(stream.getSchema());
                while (stream.next()) {
                    System.out.println(stream.getRoot().contentToTSVString());
                }
                System.out.println();
            }
            handle.value().close();
        }
        end = System.nanoTime();
        System.out.printf("%s duration%n", Duration.ofNanos(end - start));
    }
}
