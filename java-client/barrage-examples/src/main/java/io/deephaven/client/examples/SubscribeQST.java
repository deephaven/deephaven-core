//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.base.system.AsyncSystem;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.time.Duration;

@Command(name = "subscribe-qst", mixinStandardHelpOptions = true,
        description = "Send a QST, get the results, and subscribe over barrage", version = "0.1.0")
class SubscribeQST extends SubscribeExampleBase {

    TableSpec table = TimeTable.of(Duration.ofSeconds(1));

    @Override
    protected TableCreationLogic logic() {
        return table.logic();
    }

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler(AsyncSystem.uncaughtExceptionHandler(1, System.err));
        int execute = new CommandLine(new SubscribeQST()).execute(args);
        System.exit(execute);
    }
}
