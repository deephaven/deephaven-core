/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "subscribe-qst", mixinStandardHelpOptions = true,
        description = "Send a QST, get the results, and subscribe over barrage", version = "0.1.0")
class SubscribeQST extends SubscribeExampleBase {

    @Parameters(arity = "1", paramLabel = "QST", description = "QST file to send and get.",
            converter = TableConverter.class)
    TableSpec table;

    @Override
    protected TableCreationLogic logic() {
        return table.logic();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new SubscribeQST()).execute(args);
        System.exit(execute);
    }
}
