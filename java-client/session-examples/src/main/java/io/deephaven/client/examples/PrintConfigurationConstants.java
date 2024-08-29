//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.proto.backplane.grpc.ConfigValue;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Map.Entry;

@Command(name = "print-configuration-constants", mixinStandardHelpOptions = true,
        description = "Print configuration constants", version = "0.1.0")
class PrintConfigurationConstants extends SingleSessionExampleBase {

    @Override
    protected void execute(Session session) throws Exception {
        for (Entry<String, ConfigValue> entry : session.getConfigurationConstants().get().entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue().getStringValue());
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new PrintConfigurationConstants()).execute(args);
        System.exit(execute);
    }
}
