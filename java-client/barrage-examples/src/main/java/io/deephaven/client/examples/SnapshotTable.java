//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.base.system.AsyncSystem;
import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSessionFactoryConfig;
import io.deephaven.client.impl.BarrageSnapshot;
import io.deephaven.client.impl.BarrageSubscription;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.util.SafeCloseable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.BitSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Takes Barrage snapshots of a server table into client-side engine tables, in several shapes: every row and column, a
 * row range, a column subset, both, and a viewport counted from the end. The last example takes the snapshot through a
 * subscription, the cheapest way to get a consistent copy of a ticking table.
 */
@Command(name = "snapshot-table", mixinStandardHelpOptions = true,
        description = "Request a table snapshot over barrage", version = "0.1.0")
class SnapshotTable implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true)
    BatchOrSerialOptions mode;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Override
    public Void call() throws Exception {
        // Arrow memory for the data read and written over Flight
        final BufferAllocator allocator = new RootAllocator();
        // The scheduler runs the client's background work, such as refreshing the session token
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        // The factory holds the connection; each session it opens is one authenticated login on that connection
        final BarrageSessionFactoryConfig.Factory factory = BarrageSessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();

        // Snapshots land in client-side engine tables, so this process needs an engine: the DEFAULT update graph
        // must exist, and the calling thread needs an execution context. Marking the context systemic keeps it from
        // being picked up by code that should be supplying its own.
        final PeriodicUpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder("DEFAULT").existingOrBuild();
        final ExecutionContext executionContext = ExecutionContext.newBuilder()
                .markSystemic()
                .emptyQueryScope()
                .newQueryLibrary()
                .setUpdateGraph(updateGraph)
                .build();
        // A BarrageSession adds snapshots and subscriptions, which land in client-side engine tables, on top of a
        // FlightSession
        try (
                final SafeCloseable ignored = executionContext.open();
                final BarrageSession client = factory.newBarrageSession()) {
            snapshots(client);
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private void snapshots(BarrageSession client) throws Exception {
        // A ticket names a table the server already holds; table() wraps it as a TableSpec to execute again
        final TableSpec table = ticket.ticketId().table();
        // Batch sends a whole query as one request; serial sends one operation per request
        final TableHandleManager manager = BatchOrSerialOptions.manager(mode, client.session());
        final BarrageSnapshotOptions options = BarrageSnapshotOptions.builder().build();

        // Each snapshot is opened in its own liveness scope, so the client-side table is released when the scope
        // closes.

        // example #1 - verify full table reading
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TableHandle handle = manager.execute(table)) {
            final BarrageSnapshot snapshot = client.snapshot(handle, options);
            System.out.println("Requesting all rows, all columns");
            // expect this to block until all reading complete
            show(snapshot.entireTable().get());
        }

        // example #2 - reading all columns, but only subset of rows starting with 0
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TableHandle handle = manager.execute(table)) {
            final BarrageSnapshot snapshot = client.snapshot(handle, options);
            System.out.println("Requesting rows 0-5, all columns");
            final RowSet viewport = RowSetFactory.fromRange(0, 5); // range inclusive
            show(snapshot.partialTable(viewport, null).get());
        }

        // example #3 - reading all columns, but only subset of rows starting at >0
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TableHandle handle = manager.execute(table)) {
            final BarrageSnapshot snapshot = client.snapshot(handle, options);
            System.out.println("Requesting rows 6-10, all columns");
            final RowSet viewport = RowSetFactory.fromRange(6, 10); // range inclusive
            show(snapshot.partialTable(viewport, null).get());
        }

        // example #4 - reading some columns but all rows
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TableHandle handle = manager.execute(table)) {
            final BarrageSnapshot snapshot = client.snapshot(handle, options);
            System.out.println("Requesting all rows, columns 0-1");
            final BitSet columns = new BitSet();
            columns.set(0, 2); // range not inclusive (sets bits 0-1)
            show(snapshot.partialTable(null, columns).get());
        }

        // example #5 - reading some columns and only some rows
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TableHandle handle = manager.execute(table)) {
            final BarrageSnapshot snapshot = client.snapshot(handle, options);
            System.out.println("Requesting rows 100-150, columns 0-1");
            final RowSet viewport = RowSetFactory.fromRange(100, 150); // range inclusive
            final BitSet columns = new BitSet();
            columns.set(0, 2); // range not inclusive (sets bits 0-1)
            show(snapshot.partialTable(viewport, columns).get());
        }

        // example #6 - reverse viewport, all columns
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TableHandle handle = manager.execute(table);
                final RowSet viewport = RowSetFactory.flat(5)) {
            final BarrageSnapshot snapshot = client.snapshot(handle, options);
            System.out.println("Requesting rows from end 0-4, all columns");
            show(snapshot.partialTable(viewport, null, true).get());
        }

        // example #7 - reverse viewport, some columns
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TableHandle handle = manager.execute(table);
                final RowSet viewport = RowSetFactory.flat(5)) {
            final BarrageSnapshot snapshot = client.snapshot(handle, options);
            System.out.println("Requesting rows from end 0-4, columns 0-1");
            final BitSet columns = new BitSet();
            columns.set(0, 2); // range not inclusive (sets bits 0-1)
            show(snapshot.partialTable(viewport, columns, true).get());
        }

        // Example #8 - full snapshot (through BarrageSubscriptionRequest)
        // This is an example of the most efficient way to retrieve a consistent snapshot of a Deephaven table. Using
        // `snapshotPartialTable()` or `snapshotEntireTable()` will internally create a subscription and retrieve rows
        // from the server until a consistent view of the desired rows is established. Then the subscription will be
        // terminated and the table returned to the user.
        final BarrageSubscriptionOptions subOptions = BarrageSubscriptionOptions.builder().build();
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TableHandle handle = manager.execute(table)) {
            final BarrageSubscription subscription = client.subscribe(handle, subOptions);
            System.out.println("Snapshot created");
            show(subscription.snapshotEntireTable().get());
        }

        System.out.println("End of Snapshot examples");
    }

    private static void show(Table table) {
        System.out.println("Table info: rows = " + table.size() + ", cols = " + table.numColumns());
        TableTools.show(table);
        System.out.println();
        System.out.println();
    }

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler(AsyncSystem.uncaughtExceptionHandler(1, System.err));
        System.exit(new CommandLine(new SnapshotTable()).execute(args));
    }
}
