/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSubscription;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.DataAccessHelpers;
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
            System.out.println("Table info: rows = " + subscriptionTable.size() + ", cols = " +
                    DataAccessHelpers.getColumns(subscriptionTable).length);
            TableTools.show(subscriptionTable);
            System.out.println();

            subscriptionTable.addUpdateListener(listener = new InstrumentedTableUpdateListener("example-listener") {
                @ReferentialIntegrity
                final BarrageTable tableRef = subscriptionTable;
                {
                    // Maintain a liveness ownership relationship with subscriptionTable for the lifetime of the
                    // listener
                    manage(tableRef);
                }

                @Override
                protected void destroy() {
                    super.destroy();
                    tableRef.removeUpdateListener(this);
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
