package io.deephaven.client.examples;

import io.deephaven.client.impl.ApplicationFieldId;
import io.deephaven.client.impl.HasTicketId;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "get-application-table", mixinStandardHelpOptions = true,
        description = "Get an application table",
        version = "0.1.0")
class GetApplicationTable extends GetDirectTable {

    @Parameters(arity = "1", paramLabel = "VAR", description = "The application id.")
    String applicationId;

    @Parameters(arity = "1", paramLabel = "VAR", description = "The field name.")
    String fieldName;

    @Override
    public HasTicketId ticketId() {
        return new ApplicationFieldId(applicationId, fieldName);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetApplicationTable()).execute(args);
        System.exit(execute);
    }
}
