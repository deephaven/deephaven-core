package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import org.apache.arrow.vector.types.pojo.Schema;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "get-schema", mixinStandardHelpOptions = true, description = "Get a schema", version = "0.1.0")
class GetDirectSchema extends FlightExampleBase {

    enum Format {
        DEFAULT, JSON
    }

    @Option(names = {"-f", "--format"},
            description = "The output format, default: ${DEFAULT-VALUE}, candidates: [ ${COMPLETION-CANDIDATES} ]",
            defaultValue = "DEFAULT")
    Format format;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Path path;

    @Override
    protected void execute(FlightSession flight) throws Exception {
        final Schema schema = flight.schema(path);
        switch (format) {
            case DEFAULT:
                System.out.println(schema);
                break;
            case JSON:
                System.out.println(schema.toJson());
                break;
            default:
                throw new IllegalStateException("Unexpected format " + format);
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetDirectSchema()).execute(args);
        System.exit(execute);
    }
}
