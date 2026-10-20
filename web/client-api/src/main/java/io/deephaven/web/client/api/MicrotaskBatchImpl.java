//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
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

public abstract class MicrotaskBatchImpl implements JsTableOperations {

    /**
     * Internal thenable implementation that wraps an ongoing promise, and allows new calls to race that operation.
     */
    private static class Pending extends MicrotaskBatchImpl implements JsPendingTable {
        private final BatchTableRequest.Operation operation;

        public Pending(WorkerConnection connection, Ticket ticket, BatchTableRequest.Operation.Builder operation) {
            super(connection, ticket);
            this.operation = operation.build();
        }

        @Override
        public <V> PromiseLike<V> then(
                @Nullable ThenOnFulfilledCallbackFn<? super JsResolvedTable, ? extends V> onFulfilled,
                @Nullable ThenOnRejectedCallbackFn<? extends V> onRejected) {
            retained = true;
            return Js.cast(promise.asPromise().then(etcr -> {
                return Promise.resolve(new Resolved(connection, ticket, operation.toBuilder()));
            }).then(Js.cast(onFulfilled), Js.cast(onRejected)));
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
    private static class Resolved extends MicrotaskBatchImpl implements JsResolvedTable {
        private final BatchTableRequest.Operation operation;

        public Resolved(WorkerConnection connection, Ticket ticket, BatchTableRequest.Operation.Builder operation) {
            super(connection, ticket);
            this.operation = operation.build();
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
            return Promise.resolve(new Resolved(connection, ticket, operation.toBuilder()));
        }
    }

    protected enum State {
        ENQUEUED, RUNNING, RESOLVED, RELEASED, FAILED
    }

    protected final LazyPromise<ExportedTableCreationResponse> promise = new LazyPromise<>();
    protected State state = State.ENQUEUED;
    protected boolean retained;
    private static @Nullable List<Pending> enqueued;
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
        if (enqueued == null) {
            ArrayList<Pending> tasks = new ArrayList<>();
            enqueued = tasks;

            // Wait a microtask, then send all accumulated requests in a single gRPC call
            Promise.resolve((Object) null).then(ignore -> {
                assert tasks == enqueued;
                enqueued = null;

                BatchTableRequest.Builder request = BatchTableRequest.newBuilder();
                Map<Ticket, Pending> ticketToTask = new HashMap<>();
                for (int i = 0; i < tasks.size(); i++) {
                    Pending task = tasks.get(i);
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
                    getConnection().tableServiceClient().batch(request.build(), observer);
                });
                stream.onData(etcr -> {
                    Pending pending = ticketToTask.remove(etcr.getResultId().getTicket());
                    if (etcr.getSuccess()) {
                        if (pending.retained) {
                            pending.state = State.RESOLVED;
                            pending.promise.succeed(etcr);
                        } else {
                            pending.state = State.RELEASED;
                            pending.promise.fail("Resolved, but was never retained, cannot be retained now");
                            // TODO this is probably too early, other operations could still be using it. CTS has
                            // some tooling to handle this for us, probably defer to that
                            pending.connection.releaseTicket(pending.ticket);
                        }
                    } else {
                        pending.state = State.FAILED;
                        pending.promise.fail(etcr.getErrorInfo());
                    }
                });
                stream.onEnd(status -> {
                    for (Pending pending : ticketToTask.values()) {
                        pending.state = State.FAILED;
                        pending.promise.fail(status.getDescription());
                    }
                    ticketToTask.clear();
                });
                // TODO support cancelation? Otherwise we're okay losing the reference to this stream
                return null;
            });
        }
        Pending pending = new Pending(connection, resultId, operation);
        enqueued.add(pending);
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
