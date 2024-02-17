/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.DeephavenSessionTestBase;
import io.deephaven.client.impl.TableService.TableHandleFuture;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.util.SafeCloseable;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class TableServiceAsyncTest extends DeephavenSessionTestBase {

    private static final Duration GETTIME = Duration.ofSeconds(15);
    private static final int CHAIN_OPS = 250;
    private static final int CHAIN_ROWS = 1000;

    @Test(timeout = 20000)
    public void longChainAsyncExportOnlyLast() throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain();
        final TableSpec longChainLast = longChain.get(longChain.size() - 1);
        try (final TableHandle handle = get(session.executeAsync(longChainLast))) {
            checkSucceeded(handle, CHAIN_OPS);
        }
    }

    @Test(timeout = 20000)
    public void longChainAsyncExportAll() throws ExecutionException, InterruptedException, TimeoutException {
        final List<TableSpec> longChain = createLongChain();
        final List<? extends TableHandleFuture> futures = session.executeAsync(longChain);
        try {
            int chainLength = 0;
            for (final TableHandleFuture future : futures) {
                try (final TableHandle handle = get(future)) {
                    checkSucceeded(handle, ++chainLength);
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
            checkSucceeded(lastHandle, CHAIN_OPS);
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
                    checkSucceeded(handle, CHAIN_OPS);
                }
            }
        }
    }

    private static TableHandle get(TableHandleFuture future)
            throws ExecutionException, InterruptedException, TimeoutException {
        return future.getOrCancel(GETTIME);
    }

    private void checkSucceeded(TableHandle x, int chainLength) {
        assertThat(x.isSuccessful()).isTrue();
        try (final SafeCloseable ignored = getExecutionContext().open()) {
            final Table result = serverSessionState.<Table>getExport(x.exportId().id()).get();
            ExecutionContext.getContext().getQueryScope().putParam("ChainLength", chainLength);
            final Table expected = TableTools.emptyTable(CHAIN_ROWS).update("Current = ii - 1 + ChainLength");
            TstUtils.assertTableEquals(expected, result);
        }
    }

    private static List<TableSpec> createLongChain() {
        return createLongChain(CHAIN_OPS, CHAIN_ROWS);
    }

    private static List<TableSpec> createLongChain(int chainLength, int numRows) {
        final List<TableSpec> longChain = new ArrayList<>(chainLength);
        for (int i = 0; i < chainLength; ++i) {
            if (i == 0) {
                longChain.add(TableSpec.empty(numRows).view("Current = ii"));
            } else {
                final TableSpec prev = longChain.get(i - 1);
                // Note: it's important that this formula is constant with respect to "i", otherwise we'll spend a lot
                // of time compiling formulas
                longChain.add(prev.updateView("Current = 1 + Current"));
            }
        }
        return longChain;
    }
}
