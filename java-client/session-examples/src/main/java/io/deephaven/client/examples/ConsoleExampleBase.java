//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.FieldInfo;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.script.Changes;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

abstract class ConsoleExampleBase extends SingleSessionExampleBase {

    static class Type {

        @Option(names = {"--python"}, required = true, description = "Python script type")
        boolean python;

        @Option(names = {"--groovy"}, required = true, description = "Groovy script type")
        boolean groovy;

        @Option(names = {"--other"}, required = true, description = "Other script type")
        String other;

        String consoleType() {
            if (python) {
                return "python";
            }
            if (groovy) {
                return "groovy";
            }
            return other;
        }
    }

    @ArgGroup(exclusive = true, multiplicity = "1")
    Type type;

    @Override
    protected void execute(Session session) throws Exception {
        try (final ConsoleSession console = session.console(type.consoleType()).get()) {
            execute(console);
        }
    }

    protected abstract void execute(ConsoleSession consoleSession) throws Exception;

    public static String toPrettyString(Changes changes) {
        final StringBuilder sb = new StringBuilder();
        if (changes.errorMessage().isPresent()) {
            sb.append("Error: ").append(changes.errorMessage().get()).append(System.lineSeparator());
        }
        if (changes.isEmpty()) {
            sb.append("No displayable variables updated").append(System.lineSeparator());
        } else {
            for (FieldInfo fieldInfo : changes.changes().created()) {
                sb.append(fieldInfo.type().orElse("?")).append(' ').append(fieldInfo.name()).append(" = <new>")
                        .append(System.lineSeparator());
            }
            for (FieldInfo fieldInfo : changes.changes().updated()) {
                sb.append(fieldInfo.type().orElse("?")).append(' ').append(fieldInfo.name())
                        .append(" = <updated>")
                        .append(System.lineSeparator());
            }
            for (FieldInfo fieldInfo : changes.changes().removed()) {
                sb.append(fieldInfo.type().orElse("?")).append(' ').append(fieldInfo.name()).append(" <removed>")
                        .append(System.lineSeparator());
            }
        }
        return sb.toString();
    }
}
