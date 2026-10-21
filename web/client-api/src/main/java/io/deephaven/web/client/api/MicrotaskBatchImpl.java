//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.FetchTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
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

/**
 * Implements the immutable table API over a single server ticket. Operations called on any instance are collected per
 * connection and sent as one {@link BatchTableRequest} when the current microtask ends.
 */
public abstract class MicrotaskBatchImpl implements JsTableOperations {

    /**
     * The ownerless {@link JsTableOperations} over an existing table such as a {@link JsTable}, reading the table's
     * current ticket each time an operation is called. Holds no server resources of its own.
     */
    public static class View implements JsTableOperations {
        private final ServerObject source;

        public View(ServerObject source) {
            this.source = source;
        }

        // TODO This should not touch the server at all: the view should hold the source's ClientTableState, retain
        // it, and resolve() should wrap that state in a Resolved. That lands when Resolved is backed by a CTS; until
        // then, a fetch_table op in the current batch exports the same table again, at one round trip.
        @Override
        public Promise<JsImmutableTable> resolve() {
            Ticket ticket = getConnection().getTickets().newExportTicket();
            return call(ticket, BatchTableRequest.Operation.newBuilder()
                    .setFetchTable(FetchTableRequest.newBuilder()
                            .setSourceId(TableReference.newBuilder()
                                    .setTicket(typedTicket().getTicket()))
                            .setResultId(ticket)))
                    .resolve();
        }

        @Override
        public JsPendingTable call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
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
     * Internal thenable implementation that wraps an ongoing operation, and allows new calls to race that operation.
     * Retained only once awaited; if never awaited, released as soon as the server reports it created.
     */
    private static class Pending extends MicrotaskBatchImpl implements JsPendingTable {
        private final BatchTableRequest.Operation operation;

        public Pending(WorkerConnection connection, Ticket ticket, BatchTableRequest.Operation.Builder operation) {
            super(connection, ticket);
            this.operation = operation.build();
        }

        /**
         * The promise of the table the caller will receive; calling this retains the result.
         */
        private Promise<JsImmutableTable> result() {
            retained = true;
            return promise.asPromise().then(etcr -> Promise.resolve(new Resolved(connection, ticket)));
        }

        @Override
        public <V> PromiseLike<V> then(
                @Nullable ThenOnFulfilledCallbackFn<? super JsImmutableTable, ? extends V> onFulfilled,
                @Nullable ThenOnRejectedCallbackFn<? extends V> onRejected) {
            return Js.cast(result().then(Js.cast(onFulfilled), Js.cast(onRejected)));
        }

        @Override
        public Promise<JsImmutableTable> resolve() {
            return result();
        }

        @Override
        public void foo() {
            // workaround for a javadoc -> ts issue
        }

        @Override
        public void cancel() {
            // TODO send cancellation to the server (plus or minus any CTS in the mix)
            promise.fail("Operation canceled");
        }
    }

    // TODO this all needs to be done, either by wrapping JsTable or otherwise sharing details about how
    // to interact with a CTS
    private static class Resolved extends MicrotaskBatchImpl implements JsImmutableTable {
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
        public Promise<JsImmutableTable> copy() {
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
    private static final Map<WorkerConnection, List<Pending>> enqueued = new HashMap<>();
    protected final WorkerConnection connection;
    protected final Ticket ticket;

    private final Exception trace = new Exception();

    protected MicrotaskBatchImpl(WorkerConnection connection, Ticket ticket) {
        this.connection = connection;
        this.ticket = ticket;
    }

    private void checkRetained() {
        if (!retained && state != State.ENQUEUED) {
            // While this ticket might still be valid, the user had their chance to use it, notify them they should
            // have retained it earlier
            throw new IllegalStateException("Ticket " + ticket.getTicket() + " was not retained, but is now in state "
                    + state
                    + ". See the nested trace for where it was created - it should have been retained at that time",
                    trace);
        }
    }

    @Override
    public JsPendingTable call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
        checkRetained();
        return enqueue(connection, resultId, operation);
    }

    /**
     * Queues an operation on the given connection's next batch.
     */
    public static JsPendingTable enqueue(WorkerConnection connection, Ticket resultId,
            BatchTableRequest.Operation.Builder operation) {
        Pending pending = new Pending(connection, resultId, operation);
        List<Pending> tasks = enqueued.get(connection);
        if (tasks == null) {
            tasks = new ArrayList<>();
            enqueued.put(connection, tasks);
            List<Pending> batch = tasks;

            // Wait a microtask, then send all accumulated requests in a single gRPC call
            Promise.resolve((Object) null).then(ignore -> {
                List<Pending> removed = enqueued.remove(connection);
                assert removed == batch;

                BatchTableRequest.Builder request = BatchTableRequest.newBuilder();
                Map<Ticket, Pending> ticketToTask = new HashMap<>();
                for (int i = 0; i < batch.size(); i++) {
                    Pending task = batch.get(i);
                    if (task.state != State.ENQUEUED) {
                        throw new IllegalStateException("Task " + i + " was not in ENQUEUED state, but " + task.state);
                    }
                    task.state = State.RUNNING;
                    request.addOps(task.operation);
                    ticketToTask.put(task.ticket, task);
                }

                // Create a single stream for all operations, notify each individual operation of the result when it
                // finishes
                ResponseStreamWrapper<ExportedTableCreationResponse> stream = ResponseStreamWrapper.of(observer -> {
                    connection.tableServiceClient().batch(request.build(), observer);
                });
                stream.onData(etcr -> {
                    Pending task = ticketToTask.remove(etcr.getResultId().getTicket());
                    if (etcr.getSuccess()) {
                        if (task.retained) {
                            task.state = State.RESOLVED;
                            task.promise.succeed(etcr);
                        } else {
                            task.state = State.RELEASED;
                            task.promise.fail("Resolved, but was never retained, cannot be retained now");
                            // TODO this is probably too early, other operations could still be using it. CTS has
                            // some tooling to handle this for us, probably defer to that
                            task.connection.releaseTicket(task.ticket);
                        }
                    } else {
                        task.state = State.FAILED;
                        task.promise.fail(etcr.getErrorInfo());
                    }
                });
                stream.onEnd(status -> {
                    for (Pending task : ticketToTask.values()) {
                        task.state = State.FAILED;
                        task.promise.fail(status.getDescription());
                    }
                    ticketToTask.clear();
                });
                // TODO support cancelation? Otherwise we're okay losing the reference to this stream
                return null;
            });
        }
        tasks.add(pending);
        return pending;
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
