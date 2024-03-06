//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.client.impl.TableService;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "tainted", mixinStandardHelpOptions = true,
        description = "Try to execute an unreferenceable table", version = "0.1.0")
class UnreferenceableTableExample extends SingleSessionExampleBase {

    static class Mode {
        @Option(names = {"-b", "--batch"}, required = true, description = "Batch mode")
        boolean batch;

        @Option(names = {"-s", "--serial"}, required = true, description = "Serial mode")
        boolean serial;
    }

    @ArgGroup(exclusive = true)
    Mode mode;

    @Override
    protected void execute(Session session) throws Exception {
        final TableSpec r = TableSpec.empty(10).select("R=random()");
        final TableSpec rPlusOne = r.view("PlusOne=R + 1");
        final TableSpec rMinusOne = r.view("PlusOne=R - 1");
        final TableService statefulTableService = session.newStatefulTableService();
        final TableHandleManager manager = mode == null
                ? statefulTableService
                : mode.batch
                        ? statefulTableService.batch()
                        : statefulTableService.serial();
        // noinspection unused
        try (
                final TableHandle hPlusOne = manager.execute(rPlusOne);
                // this should throw an error
                final TableHandle hMinusOne = manager.execute(rMinusOne)) {
            throw new RuntimeException("Expected an \"unreferenceable table\" exception");
        } catch (IllegalArgumentException e) {
            System.out.println("Expected");
            e.printStackTrace(System.out);
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new UnreferenceableTableExample()).execute(args);
        System.exit(execute);
    }
}
