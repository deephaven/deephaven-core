/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.client.impl.*;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.qst.TableCreationLogic;
import picocli.CommandLine;

import java.util.BitSet;

abstract class SnapshotExampleBase extends BarrageClientExampleBase {

    static class Mode {
        @CommandLine.Option(names = {"-b", "--batch"}, required = true, description = "Batch mode")
        boolean batch;

        @CommandLine.Option(names = {"-s", "--serial"}, required = true, description = "Serial mode")
        boolean serial;
    }

    @CommandLine.ArgGroup(exclusive = true)
    Mode mode;

    protected abstract TableCreationLogic logic();

    @Override
    protected void execute(final BarrageSession client) throws Exception {

        final BarrageSnapshotOptions options = BarrageSnapshotOptions.builder().build();

        final TableHandleManager manager = mode == null ? client.session()
                : mode.batch ? client.session().batch() : client.session().serial();

        // example #1 - verify full table reading
        try (final TableHandle handle = manager.executeLogic(logic());
                final BarrageSnapshot snapshot = client.snapshot(handle, options)) {

            System.out.println("Requesting all rows, all columns");

            // expect this to block until all reading complete
            final BarrageTable table = snapshot.entireTable();

            System.out.println("Table info: rows = " + table.size()
                    + ", cols = " + table.numColumns());
            TableTools.show(table);
            System.out.println("");
            System.out.println("");
        }

        // example #2 - reading all columns, but only subset of rows starting with 0
        try (final TableHandle handle = manager.executeLogic(logic());
                final BarrageSnapshot snapshot = client.snapshot(handle, options)) {

            System.out.println("Requesting rows 0-5, all columns");

            // expect this to block until all reading complete
            final RowSet viewport = RowSetFactory.fromRange(0, 5); // range inclusive
            final BarrageTable table = snapshot.partialTable(viewport, null);

            System.out.println("Table info: rows = " + table.size()
                    + ", cols = " + table.numColumns());
            TableTools.show(table);
            System.out.println("");
            System.out.println("");
        }

        // example #3 - reading all columns, but only subset of rows starting at >0
        try (final TableHandle handle = manager.executeLogic(logic());
                final BarrageSnapshot snapshot = client.snapshot(handle, options)) {

            System.out.println("Requesting rows 6-10, all columns");

            // expect this to block until all reading complete
            final RowSet viewport = RowSetFactory.fromRange(6, 10); // range inclusive
            final BarrageTable table = snapshot.partialTable(viewport, null);

            System.out.println("Table info: rows = " + table.size()
                    + ", cols = " + table.numColumns());
            TableTools.show(table);
            System.out.println("");
            System.out.println("");
        }

        // example #4 - reading some columns but all rows
        try (final TableHandle handle = manager.executeLogic(logic());
                final BarrageSnapshot snapshot = client.snapshot(handle, options)) {

            System.out.println("Requesting all rows, columns 0-1");

            // expect this to block until all reading complete
            final BitSet columns = new BitSet();
            columns.set(0, 2); // range not inclusive (sets bits 0-1)

            final BarrageTable table = snapshot.partialTable(null, columns);

            System.out.println("Table info: rows = " + table.size()
                    + ", cols = " + table.numColumns());
            TableTools.show(table);
            System.out.println("");
            System.out.println("");
        }

        // example #5 - reading some columns and only some rows
        try (final TableHandle handle = manager.executeLogic(logic());
                final BarrageSnapshot snapshot = client.snapshot(handle, options)) {

            System.out.println("Requesting rows 100-150, columns 0-1");

            // expect this to block until all reading complete
            final RowSet viewport = RowSetFactory.fromRange(100, 150); // range inclusive
            final BitSet columns = new BitSet();
            columns.set(0, 2); // range not inclusive (sets bits 0-1)

            final BarrageTable table = snapshot.partialTable(viewport, columns);

            System.out.println("Table info: rows = " + table.size()
                    + ", cols = " + table.numColumns());
            TableTools.show(table);
            System.out.println("");
            System.out.println("");
        }

        // example #6 - reverse viewport, all columns
        try (final TableHandle handle = manager.executeLogic(logic());
                final RowSet viewport = RowSetFactory.flat(5); // range inclusive

                final BarrageSnapshot snapshot = client.snapshot(handle, options)) {

            System.out.println("Requesting rows from end 0-4, all columns");

            // expect this to block until all reading complete
            final BarrageTable table = snapshot.partialTable(viewport, null, true);

            System.out.println("Table info: rows = " + table.size()
                    + ", cols = " + table.numColumns());
            TableTools.show(table);
            System.out.println("");
            System.out.println("");
        }

        // example #7 - reverse viewport, some columns
        try (final TableHandle handle = manager.executeLogic(logic());
                final RowSet viewport = RowSetFactory.flat(5); // range inclusive
                final BarrageSnapshot snapshot = client.snapshot(handle, options)) {

            System.out.println("Requesting rows from end 0-4, columns 0-1");

            final BitSet columns = new BitSet();
            columns.set(0, 2); // range not inclusive (sets bits 0-1)

            // expect this to block until all reading complete
            final BarrageTable table = snapshot.partialTable(viewport, columns, true);

            System.out.println("Table info: rows = " + table.size()
                    + ", cols = " + table.numColumns());
            TableTools.show(table);
            System.out.println("");
            System.out.println("");
        }

        // Example #8 - full snapshot (through BarrageSubscriptionRequest)
        // This is an example of the most efficient way to retrieve a consistent snapshot of a Deephaven table. Using
        // `snapshotPartialTable()` or `snapshotEntireTable()` will internally create a subscription and retrieve rows
        // from the server until a consistent view of the desired rows is established. Then the subscription will be
        // terminated and the table returned to the user.
        final BarrageSubscriptionOptions subOptions = BarrageSubscriptionOptions.builder().build();

        try (final TableHandle handle = manager.executeLogic(logic());
                final BarrageSubscription subscription = client.subscribe(handle, subOptions)) {

            System.out.println("Snapshot created");

            final BarrageTable table = subscription.snapshotEntireTable();

            System.out.println(
                    "Table info: rows = " + table.size() + ", cols = " + table.getColumns().length);
            TableTools.show(table);
            System.out.println("");
        }

        System.out.println("End of Snapshot examples");

    }
}
