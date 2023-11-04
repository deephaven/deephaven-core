/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class SessionTableServicesTest extends DeephavenSessionTestBase {

    public static final boolean NOT_STATEFUL = false;
    public static final boolean STATEFUL = true;

    @Test
    public void sessionIsNotStateful() throws TableHandleException, InterruptedException {
        checkState(session, session, NOT_STATEFUL);
    }

    @Test
    public void distinctSerialManagerIsNotStateful() throws TableHandleException, InterruptedException {
        checkState(session.serial(), session.serial(), NOT_STATEFUL);
    }

    @Test
    public void distinctBatchManagerIsNotStateful() throws TableHandleException, InterruptedException {
        checkState(session.batch(), session.batch(), NOT_STATEFUL);
    }

    @Test
    public void distinctTableServicesIsNotStateful() throws TableHandleException, InterruptedException {
        checkState(session.newStatefulTableService(), session.newStatefulTableService(), NOT_STATEFUL);
    }

    @Test
    public void distinctTableServiceAsyncsIsNotStateful()
            throws InterruptedException, ExecutionException, TimeoutException {
        checkAsyncState(session.newStatefulTableService(), session.newStatefulTableService(), NOT_STATEFUL);
    }

    // this is currently broken; serial clients *can't* reliably execute the same non-trivial TableSpec DAG
    @Ignore
    @Test
    public void singleSerialManagerIsStateful() throws TableHandleException, InterruptedException {
        final TableHandleManager manager = session.serial();
        checkState(manager, manager, STATEFUL);
    }

    @Test
    public void singleBatchManagerIsStateful() throws TableHandleException, InterruptedException {
        final TableHandleManager manager = session.batch();
        checkState(manager, manager, STATEFUL);
    }

    @Test
    public void singleTableServiceIsStateful() throws TableHandleException, InterruptedException {
        final TableService ts = session.newStatefulTableService();
        checkState(ts, ts, STATEFUL);
    }

    @Test
    public void singleTableServiceAsyncIsStateful() throws InterruptedException, ExecutionException, TimeoutException {
        final TableService ts = session.newStatefulTableService();
        checkAsyncState(ts, ts, STATEFUL);
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

    private static TableSpec tableSpec() {
        final TableSpec t1 = TableSpec.empty(99).updateView("I=ii");
        final TableSpec t2 = TableSpec.empty(99).updateView("I=ii", "J=ii");
        return t1.naturalJoin(t2, "I", "J");
    }
}
