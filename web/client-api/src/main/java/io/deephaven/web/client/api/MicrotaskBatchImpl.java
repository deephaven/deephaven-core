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

public class MicrotaskBatchImpl implements JsTableOperations {
    private static class Pending extends MicrotaskBatchImpl implements JsPendingTable {
        public Pending(WorkerConnection connection, Ticket ticket, BatchTableRequest.Operation.Builder operation) {
            super(connection, ticket, operation);
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
    }
    private static class Resolved extends MicrotaskBatchImpl implements JsResolvedTable {
        public Resolved(WorkerConnection connection, Ticket ticket, BatchTableRequest.Operation.Builder operation) {
            super(connection, ticket, operation);
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
            getConnection().releaseTicket(ticket);
        }

        @Override
        public Promise<JsResolvedTable> copy() {
            return null;
        }
    }

    protected enum State {
        ENQUEUED, RUNNING, RESOLVED, RELEASED, FAILED
    }

    protected final BatchTableRequest.Operation operation;
    protected final LazyPromise<ExportedTableCreationResponse> promise = new LazyPromise<>();
    protected State state = State.ENQUEUED;
    protected boolean retained;
    private static final ThreadLocal<List<MicrotaskBatchImpl>> enqueued = new ThreadLocal<>();
    protected final WorkerConnection connection;
    protected final Ticket ticket;

    public MicrotaskBatchImpl(WorkerConnection connection, Ticket ticket,
            BatchTableRequest.Operation.Builder operation) {
        this.connection = connection;
        this.ticket = ticket;
        this.operation = operation.build();
    }

    @Override
    public JsPendingTable call(Ticket resultId, BatchTableRequest.Operation.Builder operation) {
        if (enqueued.get() == null) {
            ArrayList<MicrotaskBatchImpl> tasks = new ArrayList<>();
            enqueued.set(tasks);

            // Wait a microtask, then send all accumulated requests in a single gRPC call
            Promise.resolve((Object) null).then(ignore -> {
                assert tasks == enqueued.get();
                enqueued.remove();

                BatchTableRequest.Builder request = BatchTableRequest.newBuilder();
                Map<Ticket, MicrotaskBatchImpl> ticketToTask = new HashMap<>();
                for (int i = 0; i < tasks.size(); i++) {
                    MicrotaskBatchImpl task = tasks.get(i);
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
                    MicrotaskBatchImpl impl = ticketToTask.remove(etcr.getResultId().getTicket());
                    if (etcr.getSuccess()) {
                        if (impl.retained) {
                            impl.state = State.RESOLVED;
                            impl.promise.succeed(etcr);
                        } else {
                            impl.state = State.RELEASED;
                            impl.promise.fail("Resolved, but was never retained, cannot be retained now");
                            // TODO this is probably too early
                            impl.connection.releaseTicket(impl.ticket);
                        }
                    } else {
                        impl.state = State.FAILED;
                        impl.promise.fail(etcr.getErrorInfo());
                    }
                });
                stream.onEnd(status -> {
                    for (MicrotaskBatchImpl impl : ticketToTask.values()) {
                        impl.state = State.FAILED;
                        impl.promise.fail(status.getDescription());
                    }
                    ticketToTask.clear();
                });
                // TODO support cancelation? Otherwise we're okay losing the reference to this stream
                return null;
            });
        }
        return new Pending(connection, resultId, operation);
    }

    @Override
    public TypedTicket typedTicket() {
        return TypedTicket.newBuilder()
                .setType(JsVariableType.TABLE)
                .setTicket(ticket)
                .build();
    }

    @Override
    public TableReference tableReference() {
        return TableReference.newBuilder().setTicket(ticket).build();
    }

    @Override
    public WorkerConnection getConnection() {
        return connection;
    }
}
