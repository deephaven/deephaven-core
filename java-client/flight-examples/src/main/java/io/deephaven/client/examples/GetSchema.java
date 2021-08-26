package io.deephaven.client.examples;

import io.deephaven.client.impl.Export;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.vector.types.pojo.Schema;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.time.Duration;

@Command(name = "get-schema", mixinStandardHelpOptions = true,
        description = "Get the schema of a QST", version = "0.1.0")
class GetSchema extends FlightExampleBase {

    @Parameters(arity = "1", paramLabel = "QST", description = "QST file to send.",
            converter = TableConverter.class)
    TableSpec table;

    @Override
    protected void execute(FlightSession flight) throws Exception {
        final long start = System.nanoTime();
        final long end;
        try (final Export export = flight.session().export(table)) {
            Schema schema = flight.getSchema(export);
            end = System.nanoTime();
            System.out.println(schema);
        }
        System.out.printf("%s duration%n", Duration.ofNanos(end - start));
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetSchema()).execute(args);
        System.exit(execute);
    }
}
