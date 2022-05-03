/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.examples;

import io.deephaven.client.impl.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.util.annotations.ReferentialIntegrity;
import picocli.CommandLine;

import java.util.concurrent.CountDownLatch;

import static java.lang.Thread.sleep;

abstract class SubscribeExampleBase extends BarrageClientExampleBase {

    TableUpdateListener listener;

    @CommandLine.Option(names = {"--tail"}, required = false, description = "Tail viewport size")
    long tailSize = 0;

    @CommandLine.Option(names = {"--head"}, required = false, description = "Header viewport size")
    long headerSize = 0;

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

        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder().build();

        final TableHandleManager snapshotManager = mode == null ? client.session()
                : mode.batch ? client.session().batch() : client.session().serial();

        // This is an example of the most efficient way to retrieve a consistent snapshot from a Deephaven server. Using
        // `snapshotPartialTable()` or `snapshotEntireTable()` will internally create a subscription and retrieve rows
        // from the server until a consistent view of the table is established.  Then the subscription will be terminated
        // and the table flattened (re-indexed from 0 to size() -1) and the table presented to the user.
        try (final TableHandle handle = snapshotManager.executeLogic(logic());
                final BarrageSubscription subscription = client.subscribe(handle, options)) {

            final BarrageTable snapshotTable;
            if (headerSize > 0) {
                // create a Table snapshot with forward viewport of the specified size
                snapshotTable = subscription.snapshotPartialTable(RowSetFactory.flat(headerSize), null, false);
            } else if (tailSize > 0) {
                // create a Table snapshot with reverse viewport of the specified size
                snapshotTable = subscription.snapshotPartialTable(RowSetFactory.flat(tailSize), null, true);
            } else {
                // create a Table snapshot of the entire Table
                snapshotTable = subscription.snapshotEntireTable();
            }

            System.out.println("Snapshot created");
            System.out.println("Table info: rows = " + snapshotTable.size() + ", cols = " + snapshotTable.getColumns().length);
            TableTools.show(snapshotTable);
            System.out.println("");
        }

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final TableHandleManager subscriptionManager = mode == null ? client.session()
                : mode.batch ? client.session().batch() : client.session().serial();

        try (final TableHandle handle = subscriptionManager.executeLogic(logic());
             final BarrageSubscription subscription = client.subscribe(handle, options)) {

            final BarrageTable subscriptionTable;
            if (headerSize > 0) {
                // create a Table subscription with forward viewport of the specified size
                subscriptionTable = subscription.partialTable(RowSetFactory.flat(headerSize), null, false);
            } else if (tailSize > 0) {
                // create a Table subscription with reverse viewport of the specified size
                subscriptionTable = subscription.partialTable(RowSetFactory.flat(tailSize), null, true);
            } else {
                // create a Table subscription of the entire Table
                subscriptionTable = subscription.entireTable();
            }

            System.out.println("Subscription established");
            System.out.println("Table info: rows = " + subscriptionTable.size() + ", cols = " + subscriptionTable.getColumns().length);
            TableTools.show(subscriptionTable);
            System.out.println("");

            subscriptionTable.listenForUpdates(listener = new InstrumentedTableUpdateListener("example-listener") {
                @ReferentialIntegrity
                final BarrageTable tableRef = subscriptionTable;
                {
                    // Maintain a liveness ownership relationship with subscriptionTable for the lifetime of the listener
                    manage(tableRef);
                }

                @Override
                protected void onFailureInternal(final Throwable originalException, final Entry sourceEntry) {
                    System.out.println("exiting due to onFailureInternal:");
                    originalException.printStackTrace();
                    countDownLatch.countDown();
                }

                @Override
                public void onUpdate(final TableUpdate upstream) {
                    System.out.println("Received table update:");
                    System.out.println(upstream);
                }
            });

            countDownLatch.await();

            // For a "real" implementation, we would use liveness tracking for the listener, and ensure that it was
            // destroyed and unreachable when we no longer needed it.
            listener = null;
        }
    }
}
