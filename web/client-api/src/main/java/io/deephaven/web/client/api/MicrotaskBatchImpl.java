//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.fu.PromiseLike;
import jsinterop.base.Js;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Implements the immutable table API over a single server ticket. Operations called on any instance are collected per
 * connection and sent as one {@link BatchTableRequest} when the current microtask ends.
 */
public abstract class MicrotaskBatchImpl implements JsTableOperations {

    /**
     * The ownerless {@link JsTableOperations} over an existing table such as a {@link JsTable}, reading the table's
     * current ticket each time an operation is called. Holds no server resources of its own.
     * <p>
     * Because it reads the live ticket, a chain started on a {@link JsTable} between two client-side changes in the
     * same tick (an {@code applyFilter} followed by another before the batch flushes) can reference a state that the
     * table abandons before it is ever fetched; the server then fails the chained operation with a released dependency.
     * Chain off the state you mean to keep.
     */
    // TODO Should hold and retain the source's ClientTableState once Resolved is backed by one.
    public static class View implements JsTableOperations {
        private final ServerObject source;
        private final Supplier<Promise<? extends JsITable>> resolver;

        /**
         * @param source the table whose current ticket operations build on
         * @param resolver produces a retained, independently closeable instance of the source for {@link #resolve()};
         *        for a {@link JsTable} that is {@code copy()}, a client-side retain with no server round trip
         */
        public View(ServerObject source, Supplier<Promise<? extends JsITable>> resolver) {
            this.source = source;
            this.resolver = resolver;
        }

        @Override
        public Promise<? extends JsITable> resolve() {
            return resolver.get();
        }

        @Override
        public JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
            return enqueue(getConnection(), resultId, operation);
        }

        @Override
        public TypedTicket typedTicket() {
            return source.typedTicket();
        }

        @Override
        public WorkerConnection getConnection() {
            return source.getConnection();
        }
    }

    /**
     * Shared thenable implementation over a ticket whose table is still being created.
     *
     * @param <T> the table the caller receives on await
     */
    private abstract static class Pending<T extends JsITable> extends MicrotaskBatchImpl
            implements JsPendingTable<T> {
        protected Pending(WorkerConnection connection, Ticket ticket) {
            super(connection, ticket);
        }

        @Override
        public <V> PromiseLike<V> then(
                @Nullable ThenOnFulfilledCallbackFn<? super T, ? extends V> onFulfilled,
                @Nullable ThenOnRejectedCallbackFn<? extends V> onRejected) {
            return Js.cast(resolve().then(Js.cast(onFulfilled), Js.cast(onRejected)));
        }

        @Override
        public void foo() {
            // workaround for a javadoc -> ts issue
        }
    }

    /**
     * A pending operation sent in a batch. Retained only once awaited; if never awaited, released as soon as the server
     * reports it created.
     */
    private static class Operation extends Pending<JsResolvedTable> {
        private final BatchTableRequest.Operation operation;
        /**
         * Where this operation was created, for the error when it is used after it should have been retained.
         */
        private final Exception trace = new Exception();
        /**
         * The one resolved table this operation hands out, however many times it is awaited: one ticket, one owner.
         * TODO BEFORE MERGE: this memo, Resolved.copy() and View's deferred copy() all stand in for the reference
         * counting ClientTableState already does; back Resolved with a CTS and drop them.
         */
        private @Nullable Promise<JsResolvedTable> resolved;
        private boolean cancelled;

        public Operation(WorkerConnection connection, Ticket ticket, BatchTableRequest.Operation.Builder operation) {
            super(connection, ticket);
            this.operation = operation.build();
        }

        @Override
        public JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
            if (!retained && state != State.ENQUEUED) {
                // While this ticket might still be valid, the user had their chance to use it, notify them they should
                // have retained it earlier
                throw new IllegalStateException("Ticket " + ticket.getTicket()
                        + " was not retained, but is now in state " + state
                        + ". See the nested trace for where it was created - it should have been retained at that time",
                        trace);
            }
            return super.call(resultId, operation);
        }

        @Override
        public Promise<JsResolvedTable> resolve() {
            if (resolved == null) {
                retained = true;
                resolved = promise.asPromise().then(etcr -> Promise.resolve(new Resolved(connection, ticket)));
            }
            return resolved;
        }

        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;
            // TODO after the CTS rework: cancellation should come from an AbortController-style signal passed to
            // resolve(), and reach the server; until then, an in-flight operation still completes there and is
            // released as soon as it reports.
            switch (state) {
                case ENQUEUED:
                    // Not sent yet: the flush skips cancelled tasks
                    state = State.RELEASED;
                    break;
                case RESOLVED:
                    state = State.RELEASED;
                    connection.releaseTicket(ticket);
                    break;
                default:
                    // RUNNING: released when the response arrives; RELEASED/FAILED: nothing to do
                    break;
            }
            promise.fail("Operation canceled");
        }
    }

    /**
     * A pending table from an API that has always produced a {@link JsTable} (or another retained table) through a
     * promise: the same promise, with a ticket known up front so operations can be chained before it resolves. The
     * table is created on first await, so a pending that is only chained on retains nothing.
     */
    private static class Fetched<T extends JsITable> extends Pending<T> {
        private final Supplier<Promise<T>> fetch;
        private @Nullable Promise<T> table;

        public Fetched(WorkerConnection connection, Ticket ticket, Supplier<Promise<T>> fetch) {
            super(connection, ticket);
            this.fetch = fetch;
        }

        @Override
        public Promise<T> resolve() {
            if (table == null) {
                table = fetch.get();
            }
            return table;
        }

        @Override
        public void cancel() {
            // TODO the underlying fetch has no cancellation today; if it was awaited, the result must be closed
        }
    }

    // TODO this all needs to be done, either by wrapping JsTable or otherwise sharing details about how
    // to interact with a CTS
    private static class Resolved extends MicrotaskBatchImpl implements JsResolvedTable {
        public Resolved(WorkerConnection connection, Ticket ticket) {
            super(connection, ticket);
        }

        @Override
        public double getSize() {
            return 0;
        }

        @Override
        public ReadonlyArray<Column> getColumns() {
            return null;
        }

        @Override
        public Column findColumn(String columnName) {
            return null;
        }

        @Override
        public JsArray<String> getAttributes() {
            return null;
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @Override
        public Promise<TableData> createSnapshot(Object options) {
            return null;
        }

        @Override
        public TableViewportSubscription createViewportSubscription(Object options) {
            return null;
        }

        @Override
        public TableSubscription createSubscription(Object options) {
            return null;
        }

        @Override
        public void close() {
            state = State.RELEASED;
            retained = false;
            getConnection().releaseTicket(ticket);
        }

        @Override
        public Promise<JsResolvedTable> copy() {
            // TODO BEFORE MERGE: one ticket, two closers, no refcount: closing either copy releases the ticket for
            // both. Backing Resolved with a ClientTableState (whose bindings are refcounted, as JsTable.copy() relies
            // on) is the fix; until then this is a one-close-kills-all bug.
            return Promise.resolve(new Resolved(connection, ticket));
        }
    }

    protected enum State {
        ENQUEUED, RUNNING, RESOLVED, RELEASED, FAILED
    }

    protected final LazyPromise<ExportedTableCreationResponse> promise = new LazyPromise<>();
    protected State state = State.ENQUEUED;
    protected boolean retained;
    /**
     * Operations waiting for the current microtask to end, per connection: each connection gets its own batch request.
     */
    private static final Map<WorkerConnection, List<Operation>> enqueued = new HashMap<>();
    protected final WorkerConnection connection;
    protected final Ticket ticket;

    protected MicrotaskBatchImpl(WorkerConnection connection, Ticket ticket) {
        this.connection = connection;
        this.ticket = ticket;
    }

    @Override
    public JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
        return enqueue(connection, resultId, operation);
    }

    /**
     * Presents a table that an existing API produces through a promise as a pending table, so operations can be chained
     * on it before it resolves. The ticket must be the one the promised table will have. The supplier is invoked at
     * most once, on the first await, so a pending that is only chained on creates nothing.
     */
    public static <T extends JsITable> JsPendingTable<T> wrap(WorkerConnection connection, Ticket ticket,
            Supplier<Promise<T>> fetch) {
        return new Fetched<>(connection, ticket, fetch);
    }

    /**
     * As {@link #wrap(WorkerConnection, Ticket, Supplier)} for a fetch that has already been issued.
     */
    public static <T extends JsITable> JsPendingTable<T> wrap(WorkerConnection connection, Ticket ticket,
            Promise<T> table) {
        return new Fetched<>(connection, ticket, () -> table);
    }

    /**
     * Queues an operation on the given connection's next batch.
     */
    public static JsPendingTable<JsResolvedTable> enqueue(WorkerConnection connection, Ticket resultId,
            BatchTableRequest.Operation.Builder operation) {
        Operation pending = new Operation(connection, resultId, operation);
        List<Operation> tasks = enqueued.get(connection);
        if (tasks == null) {
            tasks = new ArrayList<>();
            enqueued.put(connection, tasks);
            List<Operation> batch = tasks;

            // Wait a microtask, then send all accumulated requests in a single gRPC call, once the server is ready
            Promise.resolve((Object) null).then(ignore -> {
                List<Operation> removed = enqueued.remove(connection);
                assert removed == batch;
                try {
                    sendBatch(connection, batch);
                } catch (Throwable t) {
                    // Nothing else observes this callback; the tasks must carry the failure
                    for (Operation task : batch) {
                        task.state = State.FAILED;
                        task.promise.fail(t);
                    }
                }
                return null;
            });
        }
        tasks.add(pending);
        return pending;
    }

    private static void sendBatch(WorkerConnection connection, List<Operation> batch) {
        BatchTableRequest.Builder request = BatchTableRequest.newBuilder();
        Map<Ticket, Operation> ticketToTask = new HashMap<>();
        for (int i = 0; i < batch.size(); i++) {
            Operation task = batch.get(i);
            if (task.cancelled) {
                continue;
            }
            if (task.state != State.ENQUEUED) {
                throw new IllegalStateException("Task " + i + " was not in ENQUEUED state, but " + task.state);
            }
            task.state = State.RUNNING;
            request.addOps(task.operation);
            ticketToTask.put(task.ticket, task);
        }
        if (ticketToTask.isEmpty()) {
            return;
        }

        connection.whenServerReady("batch table operations").then(ready -> {
            // Create a single stream for all operations, notify each individual operation of the result when it
            // finishes
            ResponseStreamWrapper<ExportedTableCreationResponse> stream = ResponseStreamWrapper.of(observer -> {
                connection.tableServiceClient().batch(request.build(), observer);
            });
            stream.onData(etcr -> {
                Operation task = ticketToTask.remove(etcr.getResultId().getTicket());
                if (!etcr.getSuccess()) {
                    task.state = State.FAILED;
                    task.promise.fail(etcr.getErrorInfo());
                } else if (task.retained && !task.cancelled) {
                    task.state = State.RESOLVED;
                    task.promise.succeed(etcr);
                } else {
                    // Never retained, or cancelled while in flight: nobody can use it now
                    task.state = State.RELEASED;
                    task.promise.fail("Resolved, but was never retained, cannot be retained now");
                    // TODO this is probably too early, other operations could still be using it. CTS has some
                    // tooling to handle this for us, probably defer to that
                    task.connection.releaseTicket(task.ticket);
                }
            });
            stream.onEnd(status -> {
                for (Operation task : ticketToTask.values()) {
                    task.state = State.FAILED;
                    task.promise.fail(status.getDescription());
                }
                ticketToTask.clear();
            });
            // TODO support cancelation? Otherwise we're okay losing the reference to this stream
            return null;
        }, err -> {
            for (Operation task : ticketToTask.values()) {
                task.state = State.FAILED;
                task.promise.fail(err);
            }
            ticketToTask.clear();
            return null;
        });
    }

    @Override
    public TypedTicket typedTicket() {
        return TypedTicket.newBuilder()
                .setType(JsVariableType.TABLE)
                .setTicket(ticket)
                .build();
    }

    @Override
    public WorkerConnection getConnection() {
        return connection;
    }
}
