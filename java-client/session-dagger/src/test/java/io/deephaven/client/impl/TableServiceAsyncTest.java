/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.DeephavenSessionTestBase;
import io.deephaven.client.impl.TableService.TableHandleFuture;
import io.deephaven.qst.table.TableSpec;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class TableServiceAsyncTest extends DeephavenSessionTestBase {

    private static final Duration GETTIME = Duration.ofSeconds(15);
    private static final int CHAIN_OPS = 50;
    private static final int CHAIN_ROWS = 1000;

    @Test(timeout = 20000)
    public void longChainAsyncExportOnlyLast() throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain();
        final TableSpec longChainLast = longChain.get(longChain.size() - 1);
        try (final TableHandle handle = get(session.executeAsync(longChainLast))) {
            checkSucceeded(handle);
        }
    }

    @Test(timeout = 20000)
    public void longChainAsyncExportAll() throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain();
        final List<? extends TableHandleFuture> futures = session.executeAsync(longChain);
        try {
            for (final TableHandleFuture future : futures) {
                try (final TableHandle handle = get(future)) {
                    checkSucceeded(handle);
                }
            }
        } catch (final Throwable t) {
            TableService.TableHandleFuture.cancelOrClose(futures, true);
            throw t;
        }
    }

    @Test(timeout = 20000)
    public void longChainAsyncExportAllCancelAllButLast()
            throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain();
        final List<? extends TableHandleFuture> futures = session.executeAsync(longChain);
        // Cancel or close all but the last one
        TableService.TableHandleFuture.cancelOrClose(futures.subList(0, futures.size() - 1), true);
        try (final TableHandle lastHandle = get(futures.get(futures.size() - 1))) {
            checkSucceeded(lastHandle);
        }
    }

    @Test(timeout = 20000)
    public void immediatelyCompletedFromCachedTableServices()
            throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain();
        final TableSpec longChainLast = longChain.get(longChain.size() - 1);
        final TableService tableService = session.newStatefulTableService();
        try (final TableHandle ignored = get(tableService.executeAsync(longChainLast))) {
            for (int i = 0; i < 1000; ++i) {
                try (final TableHandle handle = get(tableService.executeAsync(longChainLast))) {
                    checkSucceeded(handle);
                }
            }
        }
    }

    private static TableHandle get(TableHandleFuture future)
            throws ExecutionException, InterruptedException, TimeoutException {
        return future.getOrCancel(GETTIME);
    }

    private static void checkSucceeded(TableHandle x) {
        assertThat(x.isSuccessful()).isTrue();
    }

    private static List<TableSpec> createLongChain() {
        return createLongChain(CHAIN_OPS, CHAIN_ROWS);
    }

    private static List<TableSpec> createLongChain(int numColumns, int numRows) {
        final List<TableSpec> longChain = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; ++i) {
            if (i == 0) {
                longChain.add(TableSpec.empty(numRows).view("I_0=ii"));
            } else {
                final TableSpec prev = longChain.get(i - 1);
                longChain.add(prev.updateView("I_" + i + " = 1 + I_" + (i - 1)));
            }
        }
        return longChain;
    }
}
