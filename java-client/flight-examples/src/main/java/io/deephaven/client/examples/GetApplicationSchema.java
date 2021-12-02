package io.deephaven.client.examples;

import io.deephaven.client.impl.ApplicationFieldId;
import io.deephaven.client.impl.HasPathId;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "get-application-schema", mixinStandardHelpOptions = true,
        description = "Get an application schema",
        version = "0.1.0")
class GetApplicationSchema extends GetDirectSchema {

    @Parameters(arity = "1", paramLabel = "VAR", description = "The application id.")
    String applicationId;

    @Parameters(arity = "1", paramLabel = "VAR", description = "The field name.")
    String fieldName;

    @Override
    public HasPathId pathId() {
        return new ApplicationFieldId(applicationId, fieldName);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetApplicationSchema()).execute(args);
        System.exit(execute);
    }
}
