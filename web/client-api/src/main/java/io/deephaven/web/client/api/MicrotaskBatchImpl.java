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
import java.util.function.Consumer;
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
            return enqueue(source.getConnection(), resultId, operation, source.state());
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
         * Has {@code dependent} retain the state this pending table occupies until the dependent has settled.
         */
        protected abstract void retainUntilSettled(ClientTableState dependent);

        @Override
        public JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
            return enqueue(connection, resultId, operation, this::retainUntilSettled);
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
            // The fetcher re-issues this one operation. That only works while the source ticket still exists on the
            // server, i.e. a reconnect within the same session; a new session is not supported, as for
            // JsTable.refetch() (deephaven-core#3604).
            this.state = connection.newState((callback, cts) -> {
                sent = true;
                ResponseStreamWrapper<ExportedTableCreationResponse> stream = ResponseStreamWrapper.of(observer -> {
                    connection.tableServiceClient().batch(BatchTableRequest.newBuilder()
                            .addOps(this.operation)
                            .build(), observer);
                });
                boolean[] failed = {false};
                stream.onData(etcr -> {
                    // A batch reports a failed operation in-band, unlike the unary table RPCs
                    if (etcr.getSuccess()) {
                        callback.onNext(etcr);
                    } else {
                        failed[0] = true;
                        callback.onError(new RuntimeException(etcr.getErrorInfo()));
                    }
                });
                stream.onEnd(status -> {
                    if (failed[0]) {
                        return;
                    }
                    if (status.isOk()) {
                        callback.onCompleted();
                    } else {
                        callback.onError(new RuntimeException(String.valueOf(status.getDescription())));
                    }
                });
            }, "batch operation", handle);
        }

        @Override
        protected void retainUntilSettled(ClientTableState dependent) {
            MicrotaskBatchImpl.retainUntilSettled(state, dependent);
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
                    owner.close();
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
     * retain the state the promise will produce, synchronously when that state already exists.
     */
    private static class Fetched<T extends JsITable> extends Pending<T> {
        private final Supplier<Promise<T>> fetch;
        private final Consumer<ClientTableState> retainSource;
        private @Nullable Promise<T> table;

        public Fetched(WorkerConnection connection, Ticket ticket, Supplier<Promise<T>> fetch,
                Consumer<ClientTableState> retainSource) {
            super(connection, ticket);
            this.fetch = fetch;
            this.retainSource = retainSource;
        }

        @Override
        protected void retainUntilSettled(ClientTableState dependent) {
            retainSource.accept(dependent);
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

        private void checkOpen() {
            if (closed) {
                throw new IllegalStateException("Table is closed");
            }
        }

        @Override
        public JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
            checkOpen();
            return enqueue(connection, resultId, operation, state);
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
            checkOpen();
            return AbstractTableSubscription.createSnapshot(connection, state, options);
        }

        @Override
        public TableViewportSubscription createViewportSubscription(Object options) {
            checkOpen();
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
            checkOpen();
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
            checkOpen();
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
     * on it before it resolves. {@code sourceState} is the state the promised table will occupy; dependents retain it
     * from the moment they are created. The supplier is invoked at most once, on the first await.
     */
    public static <T extends JsITable> JsPendingTable<T> wrap(WorkerConnection connection,
            ClientTableState sourceState, Supplier<Promise<T>> fetch) {
        return new Fetched<>(connection, sourceState.getHandle().makeTicket(), fetch,
                dependent -> retainUntilSettled(sourceState, dependent));
    }

    /**
     * As {@link #wrap(WorkerConnection, ClientTableState, Supplier)} for a {@link JsTable} fetch that has already been
     * issued into a known state.
     */
    public static JsPendingTable<JsTable> wrapTable(WorkerConnection connection, ClientTableState sourceState,
            Promise<JsTable> table) {
        return wrap(connection, sourceState, () -> table);
    }

    /**
     * As {@link #wrap(WorkerConnection, ClientTableState, Supplier)} for a {@link JsTable} fetch that has already been
     * issued but whose state does not exist yet: the ticket must be the one the promised table will have, and
     * dependents retain the fetched table's state once the promise produces it.
     */
    public static JsPendingTable<JsTable> wrapTable(WorkerConnection connection, Ticket ticket,
            Promise<JsTable> table) {
        // The state promise is derived only when a dependent needs it, so a failed fetch that nothing chains on has no
        // unobserved rejection; the caller observes the fetch promise itself.
        return new Fetched<>(connection, ticket, () -> table, dependent -> retainUntilSettled(
                table.<ClientTableState>then(t -> Promise.resolve(t.state())), dependent));
    }

    /**
     * Queues an operation whose source state exists; the new state retains it until it has been created or has failed,
     * so the source is not released out from under it.
     */
    private static JsPendingTable<JsResolvedTable> enqueue(WorkerConnection connection, Ticket resultId,
            BatchTableRequest.Operation.Builder operation, ClientTableState sourceState) {
        return enqueue(connection, resultId, operation, dependent -> retainUntilSettled(sourceState, dependent));
    }

    /**
     * Queues an operation on the given connection's next batch. {@code retainSource} is called with the new state
     * before this returns, so the source is retained before anything else can run.
     */
    private static JsPendingTable<JsResolvedTable> enqueue(WorkerConnection connection, Ticket resultId,
            BatchTableRequest.Operation.Builder operation, Consumer<ClientTableState> retainSource) {
        Operation pending = new Operation(connection, new TableTicket(resultId), operation);
        retainSource.accept(pending.state);

        List<Operation> tasks = enqueued.get(connection);
        if (tasks == null) {
            tasks = new ArrayList<>();
            enqueued.put(connection, tasks);
            List<Operation> batch = tasks;

            // Wait a microtask, then send all accumulated requests in a single gRPC call, once the server is ready
            LazyPromise.runLater(() -> {
                List<Operation> removed = enqueued.remove(connection);
                assert removed == batch;
                sendBatch(connection, batch);
            });
        }
        tasks.add(pending);
        return pending;
    }

    /**
     * Has {@code dependent} retain {@code source} until the dependent is running, failed, or released. A dependent that
     * has already settled (possible when the source arrived through a promise) retains nothing.
     */
    private static void retainUntilSettled(ClientTableState source, ClientTableState dependent) {
        if (dependent.getResolution() != ClientTableState.ResolutionState.UNRESOLVED) {
            return;
        }
        source.retain(dependent);
        dependent.onRunning(
                ignore -> source.unretain(dependent),
                failure -> source.unretain(dependent),
                () -> source.unretain(dependent));
    }

    /**
     * As {@link #retainUntilSettled(ClientTableState, ClientTableState)} for a source that may not exist yet: the
     * retain happens when it does, and a dependent that has already settled by then retains nothing.
     */
    private static void retainUntilSettled(Promise<ClientTableState> source, ClientTableState dependent) {
        source.then(sourceState -> {
            retainUntilSettled(sourceState, dependent);
            return null;
        }, failure -> {
            // The source never came to be. Nothing here fails the dependent: it has been (or will be) sent referencing
            // a ticket the server never sees exported, and the server keeps a placeholder export waiting for it.
            // TODO BEFORE MERGE: fail the dependents of a failed source client-side instead of sending them.
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
        }).catch_(err -> {
            // The server was not ready, or setting up the stream threw: nothing else observes this, so the states
            // must carry the failure
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
