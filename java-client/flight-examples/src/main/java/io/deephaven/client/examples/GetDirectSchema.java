package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.HasPathId;
import org.apache.arrow.vector.types.pojo.Schema;
import picocli.CommandLine.Option;

abstract class GetDirectSchema extends FlightExampleBase {

    enum Format {
        DEFAULT, JSON
    }

    @Option(names = {"-f", "--format"},
            description = "The output format, default: ${DEFAULT-VALUE}, candidates: [ ${COMPLETION-CANDIDATES} ]",
            defaultValue = "DEFAULT")
    Format format;

    public abstract HasPathId pathId();

    @Override
    protected void execute(FlightSession flight) throws Exception {
        final Schema schema = flight.schema(pathId());
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
}
