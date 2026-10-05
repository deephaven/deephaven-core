//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.util.SafeCloseable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The barrage layer against a real server: snapshots into client-side engine tables, whole and by viewport, and a
 * subscription that keeps ticking.
 */
class BarrageApiTest {

    private static final BarrageSnapshotOptions SNAPSHOT = BarrageSnapshotOptions.builder().build();
    private static final BarrageSubscriptionOptions SUBSCRIPTION = BarrageSubscriptionOptions.builder().build();

    private static BufferAllocator allocator;
    private static ScheduledExecutorService scheduler;
    private static BarrageSessionFactoryConfig.Factory factory;
    private static ExecutionContext executionContext;

    @BeforeAll
    static void connect() {
        allocator = new RootAllocator();
        scheduler = Executors.newScheduledThreadPool(4);
        factory = BarrageSessionFactoryConfig.builder()
                .clientConfig(TestServer.clientConfig())
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
        // Snapshots and subscriptions land in client-side engine tables, which need an update graph and context
        final PeriodicUpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder("DEFAULT").existingOrBuild();
        executionContext = ExecutionContext.newBuilder()
                .markSystemic()
                .emptyQueryScope()
                .newQueryLibrary()
                .setUpdateGraph(updateGraph)
                .build();
    }

    @AfterAll
    static void disconnect() {
        factory.managedChannel().shutdownNow();
        scheduler.shutdownNow();
    }

    @Test
    void snapshotOfTheEntireTable() throws Exception {
        try (
                final SafeCloseable ignored = executionContext.open();
                final SafeCloseable scope = LivenessScopeStack.open();
                final BarrageSession client = factory.newBarrageSession();
                final TableHandle handle = client.session().execute(TableSpec.empty(10).view("I=ii"))) {
            final Table table = client.snapshot(handle, SNAPSHOT).entireTable().get(30, TimeUnit.SECONDS);
            assertThat(table.size()).isEqualTo(10);
            assertThat(longs(table, "I")).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        }
    }

    @Test
    void snapshotOfAViewport() throws Exception {
        try (
                final SafeCloseable ignored = executionContext.open();
                final SafeCloseable scope = LivenessScopeStack.open();
                final BarrageSession client = factory.newBarrageSession();
                final TableHandle handle = client.session().execute(TableSpec.empty(10).view("I=ii"));
                final RowSet viewport = RowSetFactory.fromRange(2, 4)) {
            final Table table =
                    client.snapshot(handle, SNAPSHOT).partialTable(viewport, null).get(30, TimeUnit.SECONDS);
            assertThat(longs(table, "I")).containsExactly(2L, 3L, 4L);
        }
    }

    @Test
    void snapshotOfAReverseViewport() throws Exception {
        try (
                final SafeCloseable ignored = executionContext.open();
                final SafeCloseable scope = LivenessScopeStack.open();
                final BarrageSession client = factory.newBarrageSession();
                final TableHandle handle = client.session().execute(TableSpec.empty(10).view("I=ii"));
                final RowSet viewport = RowSetFactory.flat(3)) {
            final Table table =
                    client.snapshot(handle, SNAPSHOT).partialTable(viewport, null, true).get(30, TimeUnit.SECONDS);
            assertThat(longs(table, "I")).containsExactly(7L, 8L, 9L);
        }
    }

    @Test
    void subscriptionKeepsTicking() throws Exception {
        try (
                final SafeCloseable ignored = executionContext.open();
                final SafeCloseable scope = LivenessScopeStack.open();
                final BarrageSession client = factory.newBarrageSession();
                final TableHandle handle = client.session().execute(TimeTable.of(Duration.ofMillis(100)))) {
            final Table table = client.subscribe(handle, SUBSCRIPTION).entireTable().get(30, TimeUnit.SECONDS);
            final long initialSize = table.size();
            final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (table.size() <= initialSize && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertThat(table.size()).as("rows after waiting for ticks").isGreaterThan(initialSize);
        }
    }

    /** The long values of a column, in row order. */
    private static List<Long> longs(Table table, String column) {
        final ColumnSource<?> source = table.getColumnSource(column);
        final List<Long> values = new ArrayList<>();
        try (final RowSet.Iterator rows = table.getRowSet().iterator()) {
            while (rows.hasNext()) {
                values.add(source.getLong(rows.nextLong()));
            }
        }
        return values;
    }
}
