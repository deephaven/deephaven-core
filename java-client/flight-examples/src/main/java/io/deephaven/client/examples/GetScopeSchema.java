package io.deephaven.client.examples;

import io.deephaven.client.impl.HasPathId;
import io.deephaven.client.impl.ScopeId;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "get-scope-schema", mixinStandardHelpOptions = true,
        description = "Get a query scope schema",
        version = "0.1.0")
class GetScopeSchema extends GetDirectSchema {

    @Parameters(arity = "1", paramLabel = "VAR", description = "The query scope variable name.")
    String variableName;

    @Override
    public HasPathId pathId() {
        return new ScopeId(variableName);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetScopeSchema()).execute(args);
        System.exit(execute);
    }
}
