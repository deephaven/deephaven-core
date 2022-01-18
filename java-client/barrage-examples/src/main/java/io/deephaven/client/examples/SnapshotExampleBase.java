/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.examples;

import io.deephaven.client.impl.*;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.qst.TableCreationLogic;
import picocli.CommandLine;

import java.util.concurrent.CountDownLatch;

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

        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder().build();

        final TableHandleManager manager = mode == null ? client.session()
                : mode.batch ? client.session().batch() : client.session().serial();

        try (final TableHandle handle = manager.executeLogic(logic());
                final BarrageSnapshot snapshot = client.snapshot(handle, options)) {

            // expect this to block until all reading complete
            final BarrageTable table = snapshot.entireTable();

            TableTools.show(table);

//
//
//
            final CountDownLatch countDownLatch = new CountDownLatch(1);
//
//            table.listenForUpdates(new InstrumentedTableUpdateListener("example-listener") {
//                @Override
//                protected void onFailureInternal(final Throwable originalException, final Entry sourceEntry) {
//                    System.out.println("exiting due to onFailureInternal:");
//                    originalException.printStackTrace();
//                    countDownLatch.countDown();
//                }
//
//                @Override
//                public void onUpdate(final TableUpdate upstream) {
//                    System.out.println("Received table update:");
//                    System.out.println(upstream);
//                }
//            });
//
            countDownLatch.await();
        }
    }
}
