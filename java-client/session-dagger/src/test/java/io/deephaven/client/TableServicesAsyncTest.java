/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client;

import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableServiceAsync.TableHandleAsync;
import io.deephaven.client.impl.TableServices;
import io.deephaven.qst.table.TableSpec;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class TableServicesAsyncTest extends DeephavenSessionTestBase {

    @Test(timeout = 15000)
    public void longChainAsyncExportOnlyLast() throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain(100, 100);
        final TableSpec longChainLast = longChain.get(longChain.size() - 1);
        handle(session.tableServices().executeAsync(longChainLast));
    }

    @Test(timeout = 15000)
    public void longChainAsyncExportAll() throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain(100, 100);
        final List<? extends TableHandleAsync> futures = session.tableServices().executeAsync(longChain);
        for (TableHandleAsync future : futures) {
            handle(future);
        }
    }

    @Test(timeout = 15000)
    public void longChainAsyncExportAllCancelAllButLast()
            throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain(100, 100);
        final List<? extends TableHandleAsync> futures = session.tableServices().executeAsync(longChain);
        // Cancel all but the last one
        {
            final Iterator<? extends TableHandleAsync> it = futures.iterator();
            while (it.hasNext()) {
                final TableHandleAsync future = it.next();
                if (it.hasNext()) {
                    future.cancel(true);
                }
            }
        }
        {
            final Iterator<? extends TableHandleAsync> it = futures.iterator();
            while (it.hasNext()) {
                final TableHandleAsync future = it.next();
                if (it.hasNext()) {
                    handleCancelledOk(future);
                } else {
                    // The last one should complete
                    handle(future);
                }
            }
        }
    }

    @Test(timeout = 15000)
    public void immediatelyCompletedFromCachedTableServices()
            throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain(100, 100);
        final TableSpec longChainLast = longChain.get(longChain.size() - 1);
        final TableServices tableServices = session.tableServices();
        try (final TableHandle ignored = get(tableServices.executeAsync(longChainLast))) {
            for (int i = 0; i < 1000; ++i) {
                final TableHandleAsync newFuture = tableServices.executeAsync(longChainLast);
                assertThat(newFuture).succeedsWithin(Duration.ZERO);
                handle(newFuture);
            }
        }
    }

    private static void handle(TableHandle x) {
        assertThat(x.isSuccessful()).isTrue();
        x.close();
    }

    private static void handle(TableHandleAsync x) throws ExecutionException, InterruptedException, TimeoutException {
        handle(get(x));
    }

    private static void handleCancelledOk(TableHandleAsync x)
            throws ExecutionException, InterruptedException, TimeoutException {
        final TableHandle handle;
        try {
            handle = get(x);
        } catch (CancellationException c) {
            // OK
            return;
        }
        handle(handle);
    }

    private static TableHandle get(TableHandleAsync x)
            throws InterruptedException, ExecutionException, TimeoutException {
        return x.get(10, TimeUnit.SECONDS);
    }

    private static List<TableSpec> createLongChain(int numColumns, int numRows) {
        final List<TableSpec> longChain = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; ++i) {
            if (i == 0) {
                longChain.add(TableSpec.empty(numRows).select("I_0=ii"));
            } else {
                final TableSpec prev = longChain.get(i - 1);
                longChain.add(prev.update("I_" + i + " = 1 + I_" + (i - 1)));
            }
        }
        return longChain;
    }
}
