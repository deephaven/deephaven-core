package io.deephaven.client.examples;

import io.deephaven.client.impl.BarrageSubscription;
import io.deephaven.client.impl.BarrageSubscriptionOptions;
import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.client.impl.table.BarrageTable;
import io.deephaven.db.v2.InstrumentedShiftAwareListener;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.util.process.ProcessEnvironment;
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

import io.deephaven.util.process.ShutdownManager;
import org.apache.commons.lang3.mutable.MutableBoolean;
import picocli.CommandLine;

import java.util.concurrent.CountDownLatch;

abstract class SubscribeExampleBase extends BarrageClientExampleBase {

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
                final BarrageSubscription subscription = client.subscribe(handle, options)) {
            final BarrageTable table = subscription.entireTable();
            final CountDownLatch countDownLatch = new CountDownLatch(1);

            table.listenForUpdates(new InstrumentedShiftAwareListener("example-listener") {
                @Override
                protected void onFailureInternal(final Throwable originalException,
                        final UpdatePerformanceTracker.Entry sourceEntry) {
                    System.out.println("exiting due to onFailureInternal:");
                    originalException.printStackTrace();
                    countDownLatch.countDown();
                }

                @Override
                public void onUpdate(final Update upstream) {
                    System.out.println("Received table update:");
                    System.out.println(upstream);
                }
            });
            ProcessEnvironment.getGlobalShutdownManager().registerTask(
                    ShutdownManager.OrderingCategory.FIRST, countDownLatch::countDown);

            countDownLatch.await();
        }
    }
}
