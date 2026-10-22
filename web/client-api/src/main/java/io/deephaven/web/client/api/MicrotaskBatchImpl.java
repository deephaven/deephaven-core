//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.web.client.api.barrage.def.TableAttributesDefinition;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.subscription.AbstractTableSubscription;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.fu.PromiseLike;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.base.Js;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Implements the immutable table API over {@link ClientTableState}s. Operations called on any instance are collected
 * per connection and sent as one {@link BatchTableRequest} when the current microtask ends; each operation owns a state
 * from the moment it is created, and the state's retainers decide when its ticket is released.
 */
public abstract class MicrotaskBatchImpl implements JsTableOperations {

    /**
     * The ownerless {@link JsTableOperations} over an existing table such as a {@link JsTable}, reading the table's
     * current ticket each time an operation is called. Holds no server resources of its own; an operation chained on it
     * retains the table's state of that moment until the operation has been created on the server.
     */
    public static class View implements JsTableOperations {
        private final JsTable source;

        public View(JsTable source) {
            this.source = source;
        }

        @Override
        public Promise<? extends JsITable> resolve() {
            return source.copy().resolve();
        }

        @Override
        public JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
            return enqueue(source.getConnection(), resultId, operation, Promise.resolve(source.state()));
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

        /**
         * The state this pending table will occupy, for a dependent operation to retain while it is created.
         */
        protected abstract Promise<ClientTableState> sourceState();

        @Override
        public JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
            return enqueue(connection, resultId, operation, sourceState());
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
     * A pending operation sent in a batch. Its {@link ClientTableState} exists from construction; the first
     * {@link #resolve()} creates the one {@link Resolved} that retains it, and a state nobody retains is released as
     * soon as its response arrives and no dependent needs it any more.
     */
    private static class Operation extends Pending<JsResolvedTable> {
        private final BatchTableRequest.Operation operation;
        private final ClientTableState state;
        private boolean sent;
        private boolean cancelled;
        private @Nullable Promise<JsResolvedTable> resolved;

        public Operation(WorkerConnection connection, TableTicket handle,
                BatchTableRequest.Operation.Builder operation) {
            super(connection, handle.makeTicket());
            this.operation = operation.build();
            // The fetcher re-issues this one operation, which is what a reconnect needs
            this.state = connection.newState((callback, cts) -> {
                sent = true;
                ResponseStreamWrapper<ExportedTableCreationResponse> stream = ResponseStreamWrapper.of(observer -> {
                    connection.tableServiceClient().batch(BatchTableRequest.newBuilder()
                            .addOps(this.operation)
                            .build(), observer);
                });
                stream.onData(callback::onNext);
                stream.onEnd(status -> {
                    if (status.isOk()) {
                        callback.onCompleted();
                    } else {
                        callback.onError(new RuntimeException(status.getDescription()));
                    }
                });
            }, "batch operation", handle);
        }

        @Override
        protected Promise<ClientTableState> sourceState() {
            return Promise.resolve(state);
        }

        @Override
        public Promise<JsResolvedTable> resolve() {
            if (resolved == null) {
                // The Resolved retains the state from now on; the caller owns it and must close it
                Resolved owner = new Resolved(connection, state);
                LazyPromise<JsResolvedTable> promise = new LazyPromise<>();
                state.onRunning(cts -> promise.succeed(owner), failure -> {
                    owner.close();
                    promise.fail(failure);
                }, () -> {
                    promise.fail("Table was released before it was created");
                });
                resolved = promise.asPromise();
            }
            return resolved;
        }

        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;
            if (resolved != null) {
                // The caller holds (or will hold) the Resolved and closes it themselves; cancellation of an operation
                // that was awaited is a no-op beyond that.
                return;
            }
            if (!sent) {
                // Never sent: nothing on the server; let the state go
                state.setResolution(ClientTableState.ResolutionState.FAILED, "Operation canceled");
            }
            // Sent and never retained: released when the response arrives, like any unretained operation.
            // TODO after the CTS rework: an AbortController-style signal passed to resolve(), reaching the server.
            connection.scheduleCheck(state);
        }
    }

    /**
     * A pending table from an API that has always produced a {@link JsTable} (or another retained table) through a
     * promise: the same promise, with a ticket known up front so operations can be chained before it resolves. The
     * table is created on first await, so a pending that is only chained on retains nothing of its own; dependents
     * retain the state the promise will produce.
     */
    private static class Fetched<T extends JsITable> extends Pending<T> {
        private final Supplier<Promise<T>> fetch;
        private final Promise<ClientTableState> sourceState;
        private @Nullable Promise<T> table;

        public Fetched(WorkerConnection connection, Ticket ticket, Supplier<Promise<T>> fetch,
                Promise<ClientTableState> sourceState) {
            super(connection, ticket);
            this.fetch = fetch;
            this.sourceState = sourceState;
        }

        @Override
        protected Promise<ClientTableState> sourceState() {
            return sourceState;
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

    /**
     * An owned, readable immutable table: one retainer on a {@link ClientTableState}. {@link #close()} drops the
     * retainer; the state and its ticket are released once nobody retains it.
     */
    private static class Resolved extends MicrotaskBatchImpl implements JsResolvedTable {
        private final ClientTableState state;
        private boolean closed;

        public Resolved(WorkerConnection connection, ClientTableState state) {
            super(connection, state.getHandle().makeTicket());
            this.state = state;
            state.retain(this);
        }

        @Override
        public JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
            if (closed) {
                throw new IllegalStateException("Table is closed");
            }
            return enqueue(connection, resultId, operation, Promise.resolve(state));
        }

        @Override
        public double getSize() {
            return state.getSize();
        }

        @Override
        public JsArray<Column> getColumns() {
            return Js.uncheckedCast(state.getColumns());
        }

        @Override
        public Column findColumn(String columnName) {
            return state.findColumn(columnName);
        }

        @Override
        public JsArray<String> getAttributes() {
            TableAttributesDefinition attrs = state.getTableDef().getAttributes();
            return Js.uncheckedCast(Stream.concat(
                    Arrays.stream(attrs.getKeys()),
                    attrs.getRemainingAttributeKeys().stream()).toArray(String[]::new));
        }

        @Override
        public Object getAttribute(String attributeName) {
            // Table-valued attributes are not supported yet, as on JsTable
            return state.getTableDef().getAttributes().getValue(attributeName);
        }

        @Override
        public Promise<TableData> createSnapshot(Object options) {
            return AbstractTableSubscription.createSnapshot(connection, state, options);
        }

        @Override
        public TableViewportSubscription createViewportSubscription(Object options) {
            DataOptions.ViewportSubscriptionOptions copy = DataOptions.ViewportSubscriptionOptions.of(options);
            if (copy.columns == null) {
                throw new IllegalArgumentException("Missing 'columns' property in viewport subscription options");
            }
            TableViewportSubscription subscription = TableViewportSubscription.make(copy, connection, state, null);
            // As on JsTable: the subscription is the caller's, independent of this table's lifetime
            subscription.retainForExternalUse();
            subscription.internalClose();
            return subscription;
        }

        @Override
        public TableSubscription createSubscription(Object options) {
            return TableSubscription.createTableSubscription(DataOptions.SubscriptionOptions.of(options), connection,
                    state);
        }

        @Override
        public void close() {
            if (closed) {
                JsLog.warn("ResolvedTable.close() called twice, second call being ignored", this);
                return;
            }
            closed = true;
            state.unretain(this);
        }

        @Override
        public Promise<JsResolvedTable> copy() {
            return Promise.resolve(new Resolved(connection, state));
        }
    }

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

    /**
     * Presents a table that an existing API produces through a promise as a pending table, so operations can be chained
     * on it before it resolves. The ticket must be the one the promised table will have, and {@code sourceState} the
     * state it will occupy, so that dependents can retain it. The supplier is invoked at most once, on the first await.
     */
    public static <T extends JsITable> JsPendingTable<T> wrap(WorkerConnection connection, Ticket ticket,
            Supplier<Promise<T>> fetch, Promise<ClientTableState> sourceState) {
        return new Fetched<>(connection, ticket, fetch, sourceState);
    }

    /**
     * As {@link #wrap(WorkerConnection, Ticket, Supplier, Promise)} for a {@link JsTable} fetch that has already been
     * issued; the source state is the fetched table's.
     */
    public static JsPendingTable<JsTable> wrapTable(WorkerConnection connection, Ticket ticket,
            Promise<JsTable> table) {
        Promise<ClientTableState> sourceState = table.<ClientTableState>then(t -> Promise.resolve(t.state()));
        return new Fetched<JsTable>(connection, ticket, () -> table, sourceState);
    }

    /**
     * Queues an operation on the given connection's next batch. The new state retains {@code sourceState} until it has
     * been created or has failed, so the source is not released out from under it.
     */
    public static JsPendingTable<JsResolvedTable> enqueue(WorkerConnection connection, Ticket resultId,
            BatchTableRequest.Operation.Builder operation, Promise<ClientTableState> sourceState) {
        Operation pending = new Operation(connection, new TableTicket(resultId), operation);
        retainUntilSettled(sourceState, pending.state);

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
                    // Nothing else observes this callback; the states must carry the failure
                    for (Operation task : batch) {
                        task.state.setResolution(ClientTableState.ResolutionState.FAILED, String.valueOf(t));
                    }
                }
                return null;
            });
        }
        tasks.add(pending);
        return pending;
    }

    /**
     * Has {@code dependent} retain {@code source} until the dependent is running, failed, or released. The source may
     * not exist yet (a fetch that has not been issued), in which case the retain happens when it does; a dependent that
     * has already settled by then releases it again at once.
     */
    private static void retainUntilSettled(Promise<ClientTableState> source, ClientTableState dependent) {
        source.then(sourceState -> {
            sourceState.retain(dependent);
            dependent.onRunning(
                    ignore -> sourceState.unretain(dependent),
                    failure -> sourceState.unretain(dependent),
                    () -> sourceState.unretain(dependent));
            return null;
        }, failure -> {
            // The source never came to be; the dependent will fail on the server in its own right
            return null;
        });
    }

    private static void sendBatch(WorkerConnection connection, List<Operation> batch) {
        BatchTableRequest.Builder request = BatchTableRequest.newBuilder();
        Map<Ticket, Operation> ticketToTask = new HashMap<>();
        for (Operation task : batch) {
            if (task.cancelled) {
                continue;
            }
            task.sent = true;
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
                if (task == null) {
                    JsLog.warn("Batch response for an unknown ticket", etcr.getResultId());
                    return;
                }
                if (etcr.getSuccess()) {
                    task.state.applyTableCreationResponse(etcr);
                } else {
                    task.state.setResolution(ClientTableState.ResolutionState.FAILED, etcr.getErrorInfo());
                }
                // If nothing retains the state (never awaited, or cancelled in flight), this releases it
                connection.scheduleCheck(task.state);
            });
            stream.onEnd(status -> {
                for (Operation task : ticketToTask.values()) {
                    task.state.setResolution(ClientTableState.ResolutionState.FAILED, status.getDescription());
                    connection.scheduleCheck(task.state);
                }
                ticketToTask.clear();
            });
            // TODO support cancelation? Otherwise we're okay losing the reference to this stream
            return null;
        }, err -> {
            for (Operation task : ticketToTask.values()) {
                task.state.setResolution(ClientTableState.ResolutionState.FAILED, String.valueOf(err));
                connection.scheduleCheck(task.state);
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
