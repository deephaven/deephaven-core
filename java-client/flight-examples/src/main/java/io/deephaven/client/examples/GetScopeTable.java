package io.deephaven.client.examples;

import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.ScopeId;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "get-scope-table", mixinStandardHelpOptions = true,
        description = "Get a query scope table",
        version = "0.1.0")
class GetScopeTable extends GetDirectTable {

    @Parameters(arity = "1", paramLabel = "VAR", description = "The query scope variable name.")
    String variableName;

    @Override
    public HasTicketId ticketId() {
        return new ScopeId(variableName);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetScopeTable()).execute(args);
        System.exit(execute);
    }
}
