//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import picocli.CommandLine.Option;

/**
 * The console script type, one of {@code --python}, {@code --groovy}, or {@code --other=<type>}.
 */
public class ScriptTypeOptions {

    @Option(names = {"--python"}, required = true, description = "Python script type")
    boolean python;

    @Option(names = {"--groovy"}, required = true, description = "Groovy script type")
    boolean groovy;

    @Option(names = {"--other"}, required = true, description = "Other script type")
    String other;

    public String consoleType() {
        if (python) {
            return "python";
        }
        if (groovy) {
            return "groovy";
        }
        return other;
    }
}
