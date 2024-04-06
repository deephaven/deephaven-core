//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.client.impl.TableService;
import io.deephaven.client.impl.TableService.TableHandleFuture;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.TableSpec;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

public class SessionTableServicesTest extends DeephavenSessionTestBase {

    public static final boolean NOT_STATEFUL = false;
    public static final boolean STATEFUL = true;

    @Test
    public void sessionIsNotStateful()
            throws TableHandleException, InterruptedException, ExecutionException, TimeoutException {
        isNotStateful(() -> session);
        isNotStatefulAsync(() -> session);
    }

    @Test
    public void sessionSerialIsNotStateful() throws TableHandleException, InterruptedException {
        isNotStateful(() -> session.serial());
    }

    @Test
    public void sessionBatchIsNotStateful() throws TableHandleException, InterruptedException {
        isNotStateful(() -> session.batch());
    }

    @Test
    public void sessionNewTsIsStateful()
            throws TableHandleException, InterruptedException, ExecutionException, TimeoutException {
        final TableService stateful = session.newStatefulTableService();
        isStateful(() -> stateful);
        isStatefulAsync(() -> stateful);
    }

    @Ignore
    @Test
    public void sessionNewTsSerialIsStateful() throws TableHandleException, InterruptedException {
        final TableService stateful = session.newStatefulTableService();
        isStateful(stateful::serial);
    }

    @Test
    public void sessionNewTsBatchIsStateful() throws TableHandleException, InterruptedException {
        final TableService stateful = session.newStatefulTableService();
        isStateful(stateful::batch);
    }

    static void isStateful(Supplier<TableHandleManager> manager) throws TableHandleException, InterruptedException {
        final TableHandleManager instance1 = manager.get();
        final TableHandleManager instance2 = manager.get();
        checkState(instance1, instance2, STATEFUL);
    }

    static void isStatefulAsync(Supplier<TableService> manager)
            throws InterruptedException, ExecutionException, TimeoutException {
        final TableService instance1 = manager.get();
        final TableService instance2 = manager.get();
        checkAsyncState(instance1, instance2, STATEFUL);
        checkExecuteAsyncStateAtSameTime2(instance1);
    }

    static void isNotStateful(Supplier<TableHandleManager> manager) throws TableHandleException, InterruptedException {
        final TableHandleManager singleInstance = manager.get();
        checkState(singleInstance, singleInstance, NOT_STATEFUL);
    }

    static void isNotStatefulAsync(Supplier<TableService> manager)
            throws InterruptedException, ExecutionException, TimeoutException {
        final TableService instance1 = manager.get();
        final TableService instance2 = manager.get();
        checkAsyncState(instance1, instance2, NOT_STATEFUL);
        checkExecuteAsyncStateAtSameTime2(instance1);
    }

    static void checkState(TableHandleManager m1, TableHandleManager m2, boolean expectEquals)
            throws TableHandleException, InterruptedException {
        checkTopLevelState(m1, m2, expectEquals);
        checkExecuteState(m1, m2, expectEquals);
    }

    static void checkAsyncState(TableService a1, TableService a2, boolean expectEquals)
            throws InterruptedException, ExecutionException, TimeoutException {
        checkExecuteAsyncState(a1, a2, expectEquals);
    }

    static void checkTopLevelState(TableCreator<TableHandle> c1, TableCreator<TableHandle> c2, boolean expectEquals) {
        try (
                final TableHandle h1 = c1.emptyTable(42);
                final TableHandle h2 = c2.emptyTable(42)) {
            assertThat(h1.exportId().toString().equals(h2.exportId().toString())).isEqualTo(expectEquals);
            try (
                    final TableHandle h3 = h1.updateView("I=ii");
                    final TableHandle h4 = h2.updateView("I=ii")) {
                assertThat(h3.exportId().toString().equals(h4.exportId().toString())).isEqualTo(expectEquals);
            }
        }
    }

    private static void checkExecuteState(TableHandleManager m1, TableHandleManager m2, boolean expectEquals)
            throws TableHandleException, InterruptedException {
        final TableSpec q = tableSpec();
        try (
                final TableHandle h1 = m1.execute(q);
                final TableHandle h2 = m2.execute(q)) {
            assertThat(h1.exportId().toString().equals(h2.exportId().toString())).isEqualTo(expectEquals);
            try (
                    final TableHandle h3 = h1.updateView("K=ii");
                    final TableHandle h4 = h2.updateView("K=ii")) {
                assertThat(h3.exportId().toString().equals(h4.exportId().toString())).isEqualTo(expectEquals);
            }
        }
    }

    private static void checkExecuteAsyncState(TableService a1, TableService a2, boolean expectEquals)
            throws InterruptedException, ExecutionException, TimeoutException {
        checkExecuteAsyncStateOneAtTime(a1, a2, expectEquals);
        checkExecuteAsyncStateAtSameTime(a1, a2, expectEquals);
    }

    private static void checkExecuteAsyncStateOneAtTime(TableService a1, TableService a2, boolean expectEquals)
            throws InterruptedException, ExecutionException, TimeoutException {
        final TableSpec q = tableSpec();
        try (
                final TableHandle h1 = a1.executeAsync(q).getOrCancel(Duration.ofSeconds(5));
                final TableHandle h2 = a2.executeAsync(q).getOrCancel(Duration.ofSeconds(5))) {
            assertThat(h1.exportId().toString().equals(h2.exportId().toString())).isEqualTo(expectEquals);
            try (
                    final TableHandle h3 = h1.updateView("K=ii");
                    final TableHandle h4 = h2.updateView("K=ii")) {
                assertThat(h3.exportId().toString().equals(h4.exportId().toString())).isEqualTo(expectEquals);
            }
        }
    }

    private static void checkExecuteAsyncStateAtSameTime(TableService a1, TableService a2, boolean expectEquals)
            throws InterruptedException, ExecutionException, TimeoutException {
        final TableSpec q = tableSpec();
        final TableHandleFuture f1 = a1.executeAsync(q);
        final TableHandleFuture f2 = a2.executeAsync(q);
        try (
                final TableHandle h1 = f1.getOrCancel(Duration.ofSeconds(5));
                final TableHandle h2 = f2.getOrCancel(Duration.ofSeconds(5))) {
            assertThat(h1.exportId().toString().equals(h2.exportId().toString())).isEqualTo(expectEquals);
            try (
                    final TableHandle h3 = h1.updateView("K=ii");
                    final TableHandle h4 = h2.updateView("K=ii")) {
                assertThat(h3.exportId().toString().equals(h4.exportId().toString())).isEqualTo(expectEquals);
            }
        }
    }

    private static void checkExecuteAsyncStateAtSameTime2(TableService a1)
            throws InterruptedException, ExecutionException, TimeoutException {
        final TableSpec q = tableSpec();
        final List<? extends TableHandleFuture> f = a1.executeAsync(List.of(q, q));
        try (
                final TableHandle h1 = f.get(0).getOrCancel(Duration.ofSeconds(555));
                final TableHandle h2 = f.get(1).getOrCancel(Duration.ofSeconds(555))) {
            assertThat(h1.exportId().toString().equals(h2.exportId().toString())).isEqualTo(true);
            try (
                    final TableHandle h3 = h1.updateView("K=ii");
                    final TableHandle h4 = h2.updateView("K=ii")) {
                assertThat(h3.exportId().toString().equals(h4.exportId().toString())).isEqualTo(true);
            }
        }
    }

    private static TableSpec tableSpec() {
        final TableSpec t1 = TableSpec.empty(99).updateView("I=ii");
        final TableSpec t2 = TableSpec.empty(99).updateView("I=ii", "J=ii");
        return t1.naturalJoin(t2, "I", "J");
    }
}
