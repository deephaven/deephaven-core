//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.base.system.AsyncSystem;
import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSessionFactoryConfig;
import io.deephaven.client.impl.BarrageSubscription;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Subscribes to a server table over Barrage, mirroring it into a client-side engine table, and prints each update as it
 * arrives. Subscribes to a one-second time table when no ticket is given.
 */
@Command(name = "subscribe-table", mixinStandardHelpOptions = true,
        description = "Request a table and subscribe over barrage", version = "0.1.0")
class SubscribeTable implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true)
    BatchOrSerialOptions mode;

    @ArgGroup(exclusive = true)
    Ticket ticket;

    @Option(names = {"--tail"}, description = "Tail viewport size")
    long tailSize = 0;

    @Option(names = {"--head"}, description = "Header viewport size")
    long headerSize = 0;

    @Option(names = {"--updates"},
            description = "The number of table updates to receive before exiting, unlimited if unset; "
                    + "0 exits after the initial snapshot")
    Long updates;

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

        // The subscription lands in a client-side engine table, so this process needs an engine: the DEFAULT update
        // graph must exist, and the calling thread needs an execution context. Marking the context systemic keeps it
        // from being picked up by code that should be supplying its own.
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
            subscribe(client);
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private void subscribe(BarrageSession client) throws Exception {
        // A ticket names a table the server already holds; without one, a TableSpec for a new time table is used
        final TableSpec table = ticket != null ? ticket.ticketId().table() : TimeTable.of(Duration.ofSeconds(1));
        // Batch sends a whole query as one request; serial sends one operation per request
        final TableHandleManager manager = BatchOrSerialOptions.manager(mode, client.session());
        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder().build();

        final long updatesToReceive = updates == null ? Long.MAX_VALUE : updates;
        final AtomicLong updatesReceived = new AtomicLong();
        final CountDownLatch done = new CountDownLatch(1);

        // When the liveness scope closes, the listener, the subscribed table, and the subscription are all released
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TableHandle handle = manager.execute(table)) {
            final BarrageSubscription subscription = client.subscribe(handle, options);

            final Table subscriptionTable;
            if (headerSize > 0) {
                // create a Table subscription with forward viewport of the specified size
                subscriptionTable = subscription.partialTable(RowSetFactory.flat(headerSize), null, false).get();
            } else if (tailSize > 0) {
                // create a Table subscription with reverse viewport of the specified size
                subscriptionTable = subscription.partialTable(RowSetFactory.flat(tailSize), null, true).get();
            } else {
                // create a Table subscription of the entire Table
                subscriptionTable = subscription.entireTable().get();
            }

            System.out.println("Subscription established");
            System.out.println("Table info: rows = " + subscriptionTable.size() + ", cols = "
                    + subscriptionTable.numColumns());
            TableTools.show(subscriptionTable);
            System.out.println();

            subscriptionTable.addUpdateListener(new InstrumentedTableUpdateListener("example-listener") {
                @ReferentialIntegrity
                final Table tableRef = subscriptionTable;
                {
                    // Maintain a liveness ownership relationship with subscriptionTable for the lifetime of the
                    // listener
                    manage(tableRef);
                }

                @OverridingMethodsMustInvokeSuper
                @Override
                protected void destroy() {
                    super.destroy();
                    tableRef.removeUpdateListener(this);
                }

                @Override
                protected void onFailureInternal(final Throwable originalException, final Entry sourceEntry) {
                    System.out.println("exiting due to onFailureInternal:");
                    originalException.printStackTrace();
                    done.countDown();
                }

                @Override
                public void onUpdate(final TableUpdate upstream) {
                    System.out.println("Received table update:");
                    System.out.println(upstream);
                    if (updatesReceived.incrementAndGet() >= updatesToReceive) {
                        done.countDown();
                    }
                }
            });

            if (updatesToReceive == 0) {
                done.countDown();
            }
            done.await();
        }
    }

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler(AsyncSystem.uncaughtExceptionHandler(1, System.err));
        System.exit(new CommandLine(new SubscribeTable()).execute(args));
    }
}
