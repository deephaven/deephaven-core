//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples.tools;

import io.deephaven.api.ColumnName;
import io.deephaven.api.TableOperations;
import io.deephaven.client.examples.AuthenticationOptions;
import io.deephaven.client.examples.BatchOrSerialOptions;
import io.deephaven.client.examples.ConnectOptions;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.qst.LabeledValue;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.TableCreationLabeledLogic;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Protocol tool: builds a small query graph in stages through a {@link TableHandleManager}, showing how batch and
 * serial modes change the number of round trips to the server. Table logic is written once against
 * {@link TableOperations}, so it is independent of whether it runs client-side or server-side.
 */
@Command(name = "table-manager", mixinStandardHelpOptions = true,
        description = "Table Manager example code", version = "0.1.0")
class TableManagerExample implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true)
    BatchOrSerialOptions mode;

    @Option(names = {"--one-stage"}, description = "Use one-stage mode")
    boolean oneStage;

    @Option(names = {"--error"}, description = "Introduce intentional error")
    boolean error;

    @Option(names = {"--line-numbers"}, description = "Mixin stacktrace line numbers")
    boolean lineNumbers;

    @Override
    public Void call() throws Exception {
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        final SessionFactoryConfig.Factory factory = SessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .scheduler(scheduler)
                .build()
                .factory();
        try (final Session session = factory.newSession()) {
            showExpectations();
            // Batch mode can optionally record line numbers for better error messages
            final TableHandleManager manager = mode != null && mode.isBatch()
                    ? session.batch(lineNumbers)
                    : BatchOrSerialOptions.manager(mode, session);
            if (oneStage) {
                executeOneStage(manager);
            } else {
                executeFourStages(manager);
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    /**
     * 7 ops
     */
    static <T extends TableOperations<T, T>> T timeStuff1(TableCreator<T> creation) {
        T t1 = creation.timeTable(Duration.ofSeconds(1)).view("I=i");
        T t2 = t1.where("I % 3 == 0").tail(3);
        T t3 = t1.where("I % 5 == 0").tail(5);
        return creation.merge(t2, t3);
    }

    /**
     * 8 ops
     */
    static <T extends TableOperations<T, T>> T timeStuff2(TableCreator<T> creation) {
        T t1 = creation.timeTable(Duration.ofSeconds(2)).view("J=i");
        T t2 = t1.where("J % 7 == 0").tail(7);
        T t3 = t1.where("J % 9 == 0").tail(9);
        return creation.merge(t2, t3).sort("J");
    }

    /**
     * 4 ops
     */
    <T extends TableOperations<T, T>> T joinStuff(T a, T b) {
        if (error) {
            return a.tail(5).join(b.reverse().head(4), Collections.emptyList(),
                    Collections.singletonList(ColumnName.of("BadColumn")));
        } else {
            return a.tail(5).join(b.reverse().head(4), Collections.emptyList(),
                    Collections.emptyList());
        }
    }

    /**
     * 3 ops
     */
    static <T extends TableOperations<T, T>> T modifyStuff(T a) {
        return a.view("IJ=I+J").sort("IJ").tail(3);
    }

    /**
     * 22 ops (7 + 8 + 4 + 3)
     */
    <T extends TableOperations<T, T>> LabeledValues<T> oneStage(TableCreator<T> creation) {
        T t1 = timeStuff1(creation);
        T t2 = timeStuff2(creation);
        T t3 = joinStuff(t1, t2);
        T t4 = modifyStuff(t3);
        return LabeledValues.<T>builder().add("t1", t1).add("t2", t2).add("t3", t3).add("t4", t4)
                .build();
    }

    /**
     * batch: 4 messages, serial: 22 messages
     */
    void executeFourStages(TableHandleManager manager)
            throws TableHandleException, InterruptedException {

        // batch or serial, based on manager
        //
        // batch: 1 message
        // serial: 7 messages
        final TableHandle t1 =
                manager.executeLogic((TableCreationLogic) TableManagerExample::timeStuff1);
        System.out.printf("Stage 1 %nt1=%s%n%n", t1.export().toReadableString());

        // batch or serial, based on manager
        //
        // batch: 1 message
        // serial: 8 messages
        final TableHandle t2 =
                manager.executeLogic((TableCreationLogic) TableManagerExample::timeStuff2);
        System.out.printf("Stage 2 %nt2=%s%n%n", t2.export().toReadableString());

        // batch or serial, based on manager
        //
        // batch: 1 message
        // serial: 4 messages
        final TableHandle t3 = manager.executeInputs(this::joinStuff, t1, t2);
        System.out.printf("Stage 3 %nt3=%s%n%n", t3.export().toReadableString());

        // batch or serial, based on manager
        //
        // batch: 1 message
        // serial: 3 messages
        final TableHandle t4 = manager.executeInputs(TableManagerExample::modifyStuff, t3);
        System.out.printf("Stage 4 %nt4=%s%n%n", t4.export().toReadableString());
    }

    /**
     * batch: 1 message, serial: 22 messages
     */
    void executeOneStage(TableHandleManager manager)
            throws TableHandleException, InterruptedException {
        // batch or serial, based on manager
        LabeledValues<TableHandle> handles =
                manager.executeLogic((TableCreationLabeledLogic) this::oneStage);
        StringBuilder sb = new StringBuilder("Stage 1").append(System.lineSeparator());
        for (LabeledValue<TableHandle> handle : handles) {
            sb.append(handle.name()).append('=').append(handle.value().export().toReadableString())
                    .append(System.lineSeparator());
        }
        System.out.println(sb);
    }

    private void showExpectations() {
        final String stages = oneStage ? "1 stage" : "4 stages";
        if (mode == null) {
            System.out.println("Executing in default mode, in " + stages + ". (1 | 22) messages expected.");
        } else if (mode.isBatch()) {
            System.out.println("Executing in explicit batch mode, in " + stages + ". 1 message expected.");
        } else {
            System.out.println("Executing in explicit serial mode, in " + stages + ". 22 messages expected.");
        }
        System.out.println();
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new TableManagerExample()).execute(args));
    }
}
