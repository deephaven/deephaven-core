package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.pojo.Field;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "list-tables", mixinStandardHelpOptions = true, description = "List the flights",
        version = "0.1.0")
class ListTables extends FlightExampleBase {

    @Option(names = {"-s", "--schema"}, description = "Whether to include schema",
            defaultValue = "false")
    boolean showSchema;

    @Override
    protected void execute(FlightSession flight) throws Exception {
        for (FlightInfo flightInfo : flight.list()) {
            if (showSchema) {
                StringBuilder sb = new StringBuilder(flightInfo.getDescriptor().toString())
                        .append(System.lineSeparator());
                for (Field field : flightInfo.getSchema().getFields()) {
                    sb.append('\t').append(field).append(System.lineSeparator());
                }
                System.out.println(sb);
            } else {
                System.out.printf("%s%n", flightInfo.getDescriptor());
            }
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new ListTables()).execute(args);
        System.exit(execute);
    }
}
