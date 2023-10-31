/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client;

import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableServiceAsync.TableHandleFuture;
import io.deephaven.client.impl.TableServices;
import io.deephaven.qst.table.TableSpec;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class TableServicesAsyncTest extends DeephavenSessionTestBase {

    private static final Duration GETTIME = Duration.ofSeconds(10);

    @Test(timeout = 15000)
    public void longChainAsyncExportOnlyLast() throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain(100, 100);
        final TableSpec longChainLast = longChain.get(longChain.size() - 1);
        try (TableHandle handle = TableHandleFuture.get(session.tableServices().executeAsync(longChainLast), GETTIME)) {
            checkSucceeded(handle);
        }
    }

    @Test(timeout = 15000)
    public void longChainAsyncExportAll() throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain(100, 100);
        final List<? extends TableHandleFuture> futures = session.tableServices().executeAsync(longChain);
        try {
            for (TableHandleFuture future : futures) {
                try (TableHandle handle = TableHandleFuture.get(future, GETTIME)) {
                    checkSucceeded(handle);
                }
            }
        } catch (Throwable t) {
            TableHandleFuture.cancelOrClose(futures, true);
            throw t;
        }
    }

    @Test(timeout = 15000)
    public void longChainAsyncExportAllCancelAllButLast()
            throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain(100, 100);
        final List<? extends TableHandleFuture> futures = session.tableServices().executeAsync(longChain);
        // Cancel or close all but the last one
        TableHandleFuture.cancelOrClose(futures.subList(0, futures.size() - 1), true);
        try (TableHandle lastHandle = TableHandleFuture.get(futures.get(futures.size() - 1), GETTIME)) {
            checkSucceeded(lastHandle);
        }
    }

    @Test(timeout = 15000)
    public void immediatelyCompletedFromCachedTableServices()
            throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain(100, 100);
        final TableSpec longChainLast = longChain.get(longChain.size() - 1);
        final TableServices tableServices = session.tableServices();
        try (TableHandle ignored = TableHandleFuture.get(tableServices.executeAsync(longChainLast), GETTIME)) {
            for (int i = 0; i < 1000; ++i) {
                try (TableHandle handle =
                        TableHandleFuture.get(tableServices.executeAsync(longChainLast), Duration.ZERO)) {
                    checkSucceeded(handle);
                }
            }
        }
    }

    private static void checkSucceeded(TableHandle x) {
        assertThat(x.isSuccessful()).isTrue();
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
