package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactory;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Command(name = "tainted", mixinStandardHelpOptions = true,
        description = "Try to execute a tainted table", version = "0.1.0")
class TaintedExample extends SingleSessionExampleBase {

    static class Mode {
        @Option(names = {"-b", "--batch"}, required = true, description = "Batch mode")
        boolean batch;

        @Option(names = {"-s", "--serial"}, required = true, description = "Serial mode")
        boolean serial;
    }

    @ArgGroup(exclusive = true)
    Mode mode;

    @Override
    protected void execute(Session session) throws Exception {
        final TableSpec t1 = TimeTable.of(Duration.ofSeconds(1));
        final TableSpec t2 = t1.view("I=i").tail(5);
        final TableSpec t3 = t1.view("J=i").tail(3);

        final TableHandleManager manager = mode == null ? session : mode.batch ? session.batch() : session.serial();

        final TableHandle handleT2 = manager.execute(t2);

        // this should throw an error
        final TableHandle handleT3 = manager.execute(t3);

        System.out.printf("t2=%s, t3=%s%n%n", handleT2.export().toReadableString(),
                handleT3.export().toReadableString());
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new TaintedExample()).execute(args);
        System.exit(execute);
    }
}
