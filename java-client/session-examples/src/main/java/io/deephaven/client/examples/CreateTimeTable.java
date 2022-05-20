package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.TableHandle;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Command(name = "create-time-table", mixinStandardHelpOptions = true,
        description = "Create a time table", version = "0.1.0")
class CreateTimeTable extends SingleSessionExampleBase {

    @Parameters(arity = "1", paramLabel = "NAME", description = "The table name.")
    String name;

    @Parameters(arity = "1", paramLabel = "TIME", description = "The tick time.")
    Duration tick;

    @Override
    protected void execute(Session session) throws Exception {
        try (final TableHandle timeTable = session.timeTable(tick)) {
            session.publish(name, timeTable).get(5, TimeUnit.SECONDS);
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new CreateTimeTable()).execute(args);
        System.exit(execute);
    }
}
