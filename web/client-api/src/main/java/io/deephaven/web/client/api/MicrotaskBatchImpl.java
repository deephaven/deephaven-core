//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.JsError;
import elemental2.dom.AbortSignal;
import elemental2.dom.DomGlobal;
import elemental2.dom.EventListener;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ColumnStatisticsRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
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
import io.deephaven.web.shared.fu.JsRunnable;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.annotations.JsOptional;
import jsinterop.base.Any;
import jsinterop.base.Js;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Implements the immutable table API over {@link ClientTableState}s. Operations called on any instance are collected
 * per connection and sent as one {@link BatchTableRequest} when the current microtask ends; each operation owns a state
 * from the moment it is created, and the state's retainers decide when its ticket is released.
 */
public abstract class MicrotaskBatchImpl implements JsTableOperations {

    static {
        // In Chromium the default is 10, and we might trim the stack too much to show where a promise is split between
        // create and await/resolve.
        if (JsError.stackTraceLimit < 64) {
            JsError.stackTraceLimit = 64;
        }
    }

    /**
     * The operations of a {@link JsTable}: a pending table over the state that table has now, which its later changes
     * do not move.
     */
    public static JsPendingTable<JsResolvedTable> operationsOf(WorkerConnection connection, ClientTableState state) {
        return new View(connection, state);
    }

    /**
     * Shared thenable implementation over a ticket whose table is still being created.
     *
     * @param <T> the table the caller receives on await
     */
    private abstract static class Pending<T extends JsITable> extends MicrotaskBatchImpl
            implements JsPendingTable<T> {
        /** Where this pending table was made, for the message when it is used too late. */
        private final JsError created = new JsError();
        private final int createdInTask = currentTask();

        protected Pending(WorkerConnection connection, Ticket ticket) {
            super(connection, ticket);
        }

        /** Set on an operation that failed before it was sent: its message already says why, and is passed on as is. */
        protected boolean failedAtBirth;

        /** Fails a dependent chained on this pending that has not settled yet, when this pending is canceled. */
        private final List<Consumer<String>> unsettled = new ArrayList<>();

        /** The state this pending table occupies. */
        protected abstract ClientTableState tableState();

        /**
         * Has the {@code dependent} state retain the state this pending table occupies until the dependent has settled,
         * and fails the dependent through {@code fail} if this pending table never produces a state, or is canceled
         * before the dependent settles. {@code role} names this table in that failure, "Source table" or "Operand
         * table".
         */
        protected void retainUntilSettled(ClientTableState dependent, Consumer<String> fail, String role) {
            MicrotaskBatchImpl.retainUntilSettled(tableState(), dependent, fail, role);
            if (dependent.getResolution() != ClientTableState.ResolutionState.UNRESOLVED) {
                return;
            }
            Consumer<String> onCancel = message -> fail.accept(role + " failed: " + message);
            unsettled.add(onCancel);
            JsRunnable stop = () -> unsettled.remove(onCancel);
            dependent.onRunning(ignore -> stop.run(), failure -> stop.run(), stop);
        }

        /** Part of every cancel: fails the dependents that have not settled, with {@code message}. */
        protected void failUnsettledDependents(String message) {
            new ArrayList<>(unsettled).forEach(fail -> fail.accept(message));
        }

        protected abstract boolean cancelled();

        /** The message this pending table fails with, once canceled. */
        protected abstract String cancelMessage();

        /** Why the server or the fetch failed this pending table, or {@code null} if it has not failed. */
        protected abstract @Nullable String failure();

        /** Names this pending table in messages. */
        protected abstract String describe();

        /**
         * The failure a use of this pending table must report now, or {@code null} if it may be used. A canceled or
         * failed pending reports that, whenever it is used. Otherwise a pending table may only be used in the task that
         * created it, awaited or not: one nobody awaited is released when its response arrives, and the table an await
         * produces is what the caller keeps. A later use fails, naming where the pending was made, once
         * {@link #currentTask()} has seen the task end; a use in a task that runs before that is not caught.
         */
        protected @Nullable String misuse() {
            if (cancelled()) {
                return cancelMessage();
            }
            String failure = failure();
            if (failure != null) {
                // It would have failed had it been used in time too, so lateness is not what went wrong
                return failure;
            }
            if (createdInTask == currentTask()) {
                return null;
            }
            String message = "A pending table (" + describe() + ") was used " + LATE_USE
                    + ". A pending table can only be awaited or chained on in the task that creates it; to use the"
                    + " table later, keep the table that awaiting it gives you, or the promise from Promise.resolve,"
                    + " and close that table when done.";
            JsLog.warn(message, created);
            return message;
        }

        /**
         * What an operation using this pending table in {@code role} ("Source table" or "Operand table") must fail with
         * now, or {@code null} if it may use it. A failure the server or the fetch reported says the table failed; a
         * cancel or a late use says the table cannot be used. An operation that already failed on the client passes its
         * own message on unchanged, so a chain does not repeat the prefix at every step.
         */
        protected @Nullable String dependentFailure(String role) {
            String misuse = misuse();
            if (misuse == null || failedAtBirth) {
                return misuse;
            }
            if (!cancelled() && failure() != null) {
                return role + " failed: " + misuse;
            }
            return "Cannot use " + role.toLowerCase() + ": " + misuse;
        }

        @Override
        public JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation,
                @Nullable JsTableReference operand) {
            String failure = dependentFailure("Source table");
            if (failure != null) {
                return failed(connection, resultId, operation, failure);
            }
            return enqueue(connection, resultId, operation,
                    dependent -> retainUntilSettled(dependent.state, dependent::cancelIfUnsettled, "Source table"),
                    operand);
        }

        /** What awaiting this pending table gives: a promise of the table it produces, retained for the caller. */
        protected abstract Promise<T> awaitResult();

        @Override
        public <V> PromiseLike<V> then(
                @Nullable ThenOnFulfilledCallbackFn<? super T, ? extends V> onFulfilled,
                @Nullable ThenOnRejectedCallbackFn<? extends V> onRejected) {
            return Js.cast(awaitResult().then(Js.cast(onFulfilled), Js.cast(onRejected)));
        }

        @Override
        public void cancel() {
            cancel("Operation canceled");
        }

        /**
         * As {@link #cancel()}, with the message the pending promise rejects with.
         */
        protected abstract void cancel(String message);

        /**
         * Cancels this pending table when the signal in {@code options} aborts, until {@code settles} does. An already
         * aborted signal cancels at once.
         */
        protected void cancelOnAbort(@Nullable Object options, Promise<?> settles) {
            AbortSignal signal = options == null ? null : new ResolveOptions(options).signal;
            if (signal == null) {
                return;
            }
            if (signal.aborted) {
                cancel(abortMessage(signal));
                return;
            }
            EventListener onAbort = e -> cancel(abortMessage(signal));
            signal.addEventListener("abort", onAbort);
            settles.then(ignore -> {
                signal.removeEventListener("abort", onAbort);
                return null;
            }, ignore -> {
                signal.removeEventListener("abort", onAbort);
                return null;
            });
        }

        private static String abortMessage(AbortSignal signal) {
            // The reason is a DOMException such as AbortError or TimeoutError; its text tells them apart
            return signal.reason == null ? "Operation canceled" : String.valueOf(signal.reason);
        }
    }

    /**
     * A pending table over one {@link ClientTableState}, which it hands out at most once, as a {@link Resolved}:
     * awaiting and resolving share that one owner. {@link #cancel()} closes the owner or rejects its promise, and fails
     * what was chained on this pending and has not settled; {@link #onCancel(String)} does whatever else the subclass
     * needs.
     */
    private abstract static class OverState extends Pending<JsResolvedTable> {
        /** Set by the subclass constructor. */
        protected ClientTableState state;
        private boolean cancelled;
        private @Nullable String cancelMessage;
        private @Nullable LazyPromise<JsResolvedTable> resolving;
        private @Nullable Promise<JsResolvedTable> resolved;
        private @Nullable Resolved owner;

        protected OverState(WorkerConnection connection, Ticket ticket) {
            super(connection, ticket);
        }

        @Override
        protected ClientTableState tableState() {
            return state;
        }

        @Override
        protected boolean cancelled() {
            return cancelled;
        }

        @Override
        protected String cancelMessage() {
            return String.valueOf(cancelMessage);
        }

        @Override
        public Promise<JsResolvedTable> resolve(@JsOptional @Nullable Object options) {
            // Checked on every call: a canceled pending's state may already be released, so its own message is the
            // answer, and after its task even an awaited pending is not to be used again
            String misuse = misuse();
            if (misuse != null) {
                return Promise.reject(misuse);
            }
            if (resolved == null) {
                // The Resolved retains the state from now on; the caller owns it and must close it, or cancel() does
                owner = new Resolved(connection, state);
                resolving = whenRunning(owner, state, describe());
                resolved = resolving.asPromise();
            }
            cancelOnAbort(options, resolved);
            return resolved;
        }

        @Override
        protected Promise<JsResolvedTable> awaitResult() {
            // A ResolvedTable either way, so awaiting and resolving share one owner
            return resolve(null);
        }

        @Override
        protected final void cancel(String message) {
            if (cancelled) {
                return;
            }
            cancelled = true;
            cancelMessage = message;
            if (owner != null) {
                // The table this pending handed out is released; if not yet delivered, its promise rejects
                owner.closeFromCancel();
                resolving.fail(message);
            }
            failUnsettledDependents(message);
            onCancel(message);
        }

        /** The rest of {@link #cancel(String)}, once the owner and the dependents have been dealt with. */
        protected abstract void onCancel(String message);
    }

    /**
     * The operations of a {@link JsTable}: a pending table over the state that table had when they were taken. It holds
     * that state until the end of the task that took it, by when anything chained on it or resolved from it holds the
     * state itself; like any pending table it is not to be used after that task. The state stays the table's, so a
     * cancel does not fail it.
     */
    private static class View extends OverState {
        private boolean holding = true;

        View(WorkerConnection connection, ClientTableState state) {
            super(connection, state.getHandle().makeTicket());
            this.state = state;
            state.retain(this);
            atTaskEnd(this::letGo);
        }

        private void letGo() {
            if (holding) {
                holding = false;
                state.unretain(this);
            }
        }

        @Override
        protected @Nullable String failure() {
            // The table's state can fail after the view was taken
            return state.getFailure();
        }

        @Override
        protected String describe() {
            return "operations()";
        }

        @Override
        protected void onCancel(String message) {
            letGo();
        }
    }

    /**
     * A pending operation sent in a batch. Its {@link ClientTableState} exists from construction; the first
     * {@link #resolve()} creates the one {@link Resolved} that retains it, and a state nobody retains is released as
     * soon as its response arrives and no dependent needs it any more. {@link #cancel()} fails the state at once: the
     * refcount then releases the ticket, and the batch stops waiting for the response.
     */
    private static class Operation extends OverState {
        private final BatchTableRequest.Operation operation;
        private boolean sent;
        /** The connection's session when the batch sent this operation; only that session can re-issue it. */
        private int sentInSession;
        private @Nullable String failure;
        private @Nullable Batch batch;

        public Operation(WorkerConnection connection, TableTicket handle,
                BatchTableRequest.Operation.Builder operation) {
            super(connection, handle.makeTicket());
            this.operation = operation.build();
            // The fetcher re-issues this one operation. That only works while the source ticket still exists on the
            // server, i.e. a reconnect within the same session; a new session is not supported, as for
            // JsTable.refetch() (deephaven-core#3604).
            this.state = connection.newState((callback, cts) -> {
                if (connection.getSessions() != sentInSession) {
                    // The new session would park the request on a source ticket nothing may define, so fail instead
                    callback.onError(new IllegalStateException(
                            "A table made by a table operation cannot be revived in a new session"));
                    return;
                }
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
        protected @Nullable String failure() {
            return failure;
        }

        /**
         * Fails this operation for a reason from the server or its stream. Kept here as well as on the state, which an
         * unretained failed state does not keep once it is released.
         */
        void fail(String message) {
            failure = message;
            state.setResolution(ClientTableState.ResolutionState.FAILED, message);
        }

        @Override
        protected String describe() {
            return operation.getOpCase().name().toLowerCase();
        }

        /**
         * As {@link #cancel(String)}, but only while this operation has not settled: once it has its table, a failure
         * elsewhere must not close a table its caller owns.
         */
        void cancelIfUnsettled(String message) {
            if (state.getResolution() == ClientTableState.ResolutionState.UNRESOLVED) {
                cancel(message);
            }
        }

        @Override
        protected void onCancel(String message) {
            if (state.getResolution() != ClientTableState.ResolutionState.UNRESOLVED) {
                // Already settled: nothing to cancel on the server
                return;
            }
            if (sent && batch != null) {
                // Stop waiting for the response; the batch closes its stream once nobody on it is waiting
                batch.settled(this);
            }
            // If never sent, sendBatch skips it, but its ticket is released all the same: a call outside the batch may
            // already have named it, and the server is to remember a release for an id it has not seen (DH-24231)
            // Fails the promise from resolve() and closes its owner, if any; the refcount then releases the ticket,
            // which cancels the work on the server if it has not begun. Work the server has already started runs to
            // completion there, so callers that cancel often should debounce.
            state.setResolution(ClientTableState.ResolutionState.FAILED, message);
            connection.scheduleCheck(state);
        }
    }

    /**
     * A pending table from an API that has always produced a {@link JsTable} (or another retained table): a state whose
     * fetch is already issued, and a way to build the table over it once the fetch has completed. The table is built on
     * the first await only, so a pending that is merely chained on holds nothing past its dependents; one that is
     * neither awaited nor chained on is released when its fetch completes, as an unawaited operation is.
     * {@link #resolve()} instead retains the state in a {@link Resolved}, and no table is built.
     */
    private static class Fetched<T extends JsITable> extends Pending<T> {
        private final ClientTableState state;
        private final Function<ClientTableState, T> build;
        private boolean holding;
        private @Nullable ClientTableState fetchedState;
        private @Nullable Object fetchFailure;
        private @Nullable LazyPromise<T> result;
        private @Nullable T delivered;
        /** Set by resolve(), which produces a ResolvedTable instead; a pending is awaited or resolved, never both. */
        private @Nullable LazyPromise<JsResolvedTable> resolving;
        private @Nullable Resolved owner;
        private boolean cancelled;
        private @Nullable String cancelMessage;

        public Fetched(WorkerConnection connection, ClientTableState state, Promise<ClientTableState> fetched,
                Function<ClientTableState, T> build) {
            super(connection, state.getHandle().makeTicket());
            this.state = state;
            this.build = build;
            // Hold the state until the fetch settles, so an await or a chained operation can still find it
            state.retain(this);
            holding = true;
            // Observe the fetch whether or not anyone awaits, so its failure is never an unhandled rejection
            fetched.then(cts -> {
                fetchedState = cts;
                if (result != null && !cancelled) {
                    deliver();
                } else {
                    letGo();
                }
                return null;
            }, failure -> {
                fetchFailure = failure;
                // The state carries the failure too, so dependents learn of it even if the fetch never reached the
                // server; a dependent the server did see fails there as well, whichever comes first
                if (state.getResolution() == ClientTableState.ResolutionState.UNRESOLVED) {
                    state.setResolution(ClientTableState.ResolutionState.FAILED, String.valueOf(failure));
                }
                if (result != null && !cancelled) {
                    result.fail(failure);
                }
                letGo();
                return null;
            });
        }

        private void letGo() {
            if (holding) {
                holding = false;
                state.unretain(this);
            }
        }

        /** Builds the table over the fetched state for the awaiter; that table then owns the state. */
        private void deliver() {
            assert result != null && fetchedState != null;
            if (delivered != null) {
                return;
            }
            if (state.getResolution() == ClientTableState.ResolutionState.RELEASED) {
                // Nobody awaited or chained on it in time; it was released when the fetch completed
                result.fail(releasedBefore(describe(), "awaited"));
                return;
            }
            delivered = build.apply(fetchedState);
            letGo();
            result.succeed(delivered);
        }

        @Override
        protected ClientTableState tableState() {
            return state;
        }

        @Override
        protected boolean cancelled() {
            return cancelled;
        }

        @Override
        protected String cancelMessage() {
            return String.valueOf(cancelMessage);
        }

        @Override
        protected @Nullable String failure() {
            return fetchFailure == null ? null : String.valueOf(fetchFailure);
        }

        /** As {@link Operation#cancelIfUnsettled(String)}. */
        void cancelIfUnsettled(String message) {
            if (state.getResolution() == ClientTableState.ResolutionState.UNRESOLVED) {
                cancel(message);
            }
        }

        @Override
        protected String describe() {
            return state.getFetchSummary();
        }

        private String alreadyTaken(String how) {
            return "A pending table (" + describe() + ") was already " + how
                    + "; await it or call resolve(), not both.";
        }

        @Override
        protected Promise<T> awaitResult() {
            String misuse = misuse();
            if (misuse != null) {
                return Promise.reject(misuse);
            }
            if (resolving != null) {
                return Promise.reject(alreadyTaken("resolved"));
            }
            if (result == null) {
                // The pending owns the promise it hands out, so a cancel can reject it for every awaiter
                result = new LazyPromise<>();
                if (fetchedState != null) {
                    deliver();
                } else if (fetchFailure != null) {
                    result.fail(fetchFailure);
                }
                // Otherwise the fetch is still in flight and delivers when it completes
            }
            return result.asPromise();
        }

        @Override
        public Promise<JsResolvedTable> resolve(@JsOptional @Nullable Object options) {
            String misuse = misuse();
            if (misuse != null) {
                return Promise.reject(misuse);
            }
            if (result != null) {
                return Promise.reject(alreadyTaken("awaited"));
            }
            if (resolving == null) {
                if (state.getResolution() == ClientTableState.ResolutionState.RELEASED) {
                    // Nobody awaited or chained on it in time; it was released when the fetch completed
                    return Promise.reject(releasedBefore(describe(), "awaited"));
                }
                // The Resolved retains the state from now on, past the fetch; the caller owns it, or cancel() does
                owner = new Resolved(connection, state);
                resolving = whenRunning(owner, state, describe());
            }
            Promise<JsResolvedTable> promise = resolving.asPromise();
            cancelOnAbort(options, promise);
            return promise;
        }

        @Override
        protected void cancel(String message) {
            if (cancelled) {
                return;
            }
            cancelled = true;
            cancelMessage = message;
            failUnsettledDependents(message);
            if (delivered != null) {
                // Already delivered: nothing to cancel, but the table this pending handed out is released
                delivered.close();
                return;
            }
            if (owner != null) {
                // Delivered or not, the resolved table is closed; if not yet delivered, its promise rejects
                owner.closeFromCancel();
                resolving.fail(message);
            }
            // Failing the state fails the operations chained on it, as an operation's cancel does, and they let go of
            // it; once this pending lets go too, nothing holds it and its ticket is released. Its call is ended too,
            // or not made if it has not been sent yet.
            if (state.getResolution() == ClientTableState.ResolutionState.UNRESOLVED) {
                state.setResolution(ClientTableState.ResolutionState.FAILED, message);
                state.cancelFetch();
            }
            if (result != null) {
                result.fail(message);
            }
            letGo();
        }
    }

    /**
     * An owned, readable immutable table: one retainer on a {@link ClientTableState}. {@link #close()} drops the
     * retainer; the state and its ticket are released once nobody retains it.
     */
    private static class Resolved extends MicrotaskBatchImpl implements JsResolvedTable {
        private final ClientTableState state;
        private boolean closed;
        private boolean closedByCancel;

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
        public JsPendingTable<JsResolvedTable> call(Ticket resultId, BatchTableRequest.Operation.Builder operation,
                @Nullable JsTableReference operand) {
            checkOpen();
            return enqueue(connection, resultId, operation, state, operand);
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
        public Promise<JsColumnStatistics> getColumnStatistics(Column.ColumnOrName column) {
            checkOpen();
            Ticket ticket = connection.getTickets().newExportTicket();
            // Awaited in this task, so the one-row statistics table is held until it is read, then closed
            Promise<JsResolvedTable> statistics = call(ticket, BatchTableRequest.Operation.newBuilder()
                    .setColumnStatistics(ColumnStatisticsRequest.newBuilder()
                            .setResultId(ticket)
                            .setSourceId(TableReference.newBuilder().setTicket(typedTicket().getTicket()))
                            .setColumnName(column.columnName())))
                    .resolve();
            return statistics.then(table -> {
                DataOptions.SnapshotOptions options = new DataOptions.SnapshotOptions();
                options.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 0));
                options.columns = Js.cast(table.getColumns());
                return table.createSnapshot(options).then(data -> {
                    table.close();
                    return Promise.resolve(new JsColumnStatistics(data));
                }, failure -> {
                    table.close();
                    return Promise.reject(failure);
                });
            });
        }

        @Override
        public Promise<Double> seekRow(double startingRow, Column.ColumnOrName column, String valueType, Any seekValue,
                @JsOptional @Nullable Boolean insensitive, @JsOptional @Nullable Boolean contains,
                @JsOptional @Nullable Boolean isBackwards) {
            checkOpen();
            return JsTable.seekRow(connection, typedTicket().getTicket(), startingRow, column, valueType, seekValue,
                    insensitive, contains, isBackwards);
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
                if (!closedByCancel) {
                    JsLog.warn("ResolvedTable.close() called twice, second call being ignored", this);
                }
                return;
            }
            closed = true;
            state.unretain(this);
        }

        /**
         * Closes on behalf of the pending table that produced this one; a later {@link #close()} by the caller is then
         * expected and not warned about.
         */
        void closeFromCancel() {
            if (!closed) {
                closedByCancel = true;
                close();
            }
        }

        @Override
        public Promise<JsResolvedTable> resolve(@JsOptional @Nullable Object options) {
            // Nothing pending to cancel, so the signal is not used
            checkOpen();
            return Promise.resolve(this);
        }

        @Override
        public JsResolvedTable copy() {
            checkOpen();
            return new Resolved(connection, state);
        }

        @Override
        public JsTable createMutableTable() {
            checkOpen();
            // The Table binds to the state itself, so each can be closed without the other
            return new JsTable(connection, state);
        }
    }

    /**
     * One batch request: the operations collected in a microtask, and once sent, those still waiting for a response and
     * the stream the responses arrive on. The stream is a browser resource (idle HTTP/2 streams are limited), so it is
     * closed as soon as no operation on it is still wanted, even while the server is working on canceled ones.
     */
    private static class Batch {
        private final List<Operation> ops = new ArrayList<>();
        private final Map<Ticket, Operation> outstanding = new HashMap<>();
        private @Nullable ResponseStreamWrapper<ExportedTableCreationResponse> stream;

        /**
         * {@code op} no longer needs a response, because it was answered or canceled. Closes the stream once nothing on
         * it does.
         */
        void settled(Operation op) {
            outstanding.remove(op.ticket);
            if (outstanding.isEmpty() && stream != null) {
                ResponseStreamWrapper<?> open = stream;
                stream = null;
                open.cancel();
            }
        }
    }

    /**
     * Operations waiting for the current microtask to end, per connection: each connection gets its own batch request.
     */
    private static final Map<WorkerConnection, Batch> enqueued = new HashMap<>();

    private static final String LATE_USE = "after the task that created it";

    /** Counts tasks (not microtasks) that created a pending table, so a later use can tell it is in another task. */
    private static int task;
    private static boolean taskEndScheduled;

    /**
     * Identifies the current task, approximately. A thenable's {@code then} is called a microtask after {@code await}
     * or {@code Promise.resolve} sees it, so the boundary that matters is the task: responses arrive in later tasks.
     * The count moves when a zero-delay timer runs, so tasks queued ahead of it (network events, messages, input,
     * animation frames) still count as the task before; background tabs throttle timers, which widens that window.
     */
    private static int currentTask() {
        if (!taskEndScheduled) {
            taskEndScheduled = true;
            DomGlobal.setTimeout(ignore -> {
                taskEndScheduled = false;
                task++;
                List<Runnable> ending = new ArrayList<>(atTaskEnd);
                atTaskEnd.clear();
                ending.forEach(Runnable::run);
            }, 0);
        }
        return task;
    }

    private static final List<Runnable> atTaskEnd = new ArrayList<>();

    /**
     * Runs {@code action} when {@link #currentTask()} sees the current task end, so that whatever the task check still
     * lets through has not yet been undone by it. Actions must not throw: one that does skips the rest.
     */
    private static void atTaskEnd(Runnable action) {
        currentTask();
        atTaskEnd.add(action);
    }

    protected final WorkerConnection connection;
    protected final Ticket ticket;

    protected MicrotaskBatchImpl(WorkerConnection connection, Ticket ticket) {
        this.connection = connection;
        this.ticket = ticket;
    }

    /**
     * Presents a fetched state as a pending table, so operations can be chained on it before the fetch completes.
     * {@code fetched} is the already-issued fetch of {@code state}; {@code build} makes the table the first awaiter
     * receives, over the fetched state, and is called at most once. A pending that nobody awaits or chains on is
     * released when the fetch completes.
     */
    public static <T extends JsITable> JsPendingTable<T> wrap(WorkerConnection connection, ClientTableState state,
            Promise<ClientTableState> fetched, Function<ClientTableState, T> build) {
        return new Fetched<>(connection, state, fetched, build);
    }

    /**
     * As {@link #wrap}, for a state not yet fetched whose request names {@code operands}' tickets outside the batch.
     * The operands are checked and held as a batch operation's are: if one cannot be used, the pending fails and
     * nothing is sent; otherwise each is held until the state settles, and one that fails first fails the pending.
     */
    public static <T extends JsITable> JsPendingTable<T> wrapWithOperands(WorkerConnection connection,
            ClientTableState state, JsTableReference[] operands, Function<ClientTableState, T> build) {
        String failure = foreignOperand(connection, operands);
        if (failure == null) {
            failure = operandFailure(operands);
        }
        if (failure != null) {
            // Never fetched, so there is no ticket to release on the server
            state.getHandle().setConnected(false);
            state.setResolution(ClientTableState.ResolutionState.FAILED, failure);
            Fetched<T> failed = new Fetched<>(connection, state, Promise.reject(failure), build);
            failed.fetchFailure = failure;
            failed.failedAtBirth = true;
            return failed;
        }
        LazyPromise<ClientTableState> fetched = new LazyPromise<>();
        Fetched<T> pending = new Fetched<>(connection, state, fetched.asPromise(), build);
        // Held before the request is sent, so no operand is released before the server sees it named
        for (JsTableReference operand : operands) {
            holdOperand(connection, operand, state, pending::cancelIfUnsettled);
        }
        if (state.getResolution() == ClientTableState.ResolutionState.UNRESOLVED) {
            state.refetch().then(cts -> {
                fetched.succeed(cts);
                return null;
            }, err -> {
                fetched.fail(err);
                return null;
            });
        }
        return pending;
    }

    /**
     * Why a request naming {@code operands}' tickets outside the batch must fail now, or {@code null} if it may be
     * sent. A pending operand that was canceled, failed, or used after its task may never define its ticket on the
     * server, so the request would wait forever.
     */
    public static @Nullable String operandFailure(JsTableReference... operands) {
        for (JsTableReference operand : operands) {
            Pending<?> pending = asPending(operand);
            String failure = pending == null ? null : pending.dependentFailure("Operand table");
            if (failure != null) {
                return failure;
            }
        }
        return null;
    }

    /**
     * Makes the gRPC call {@code call} returns, holding the tables {@code tickets} name until it settles, for a request
     * outside the batch that names them. The result is the call's, unless a held table fails or is released first: the
     * server may then never answer, so the result rejects at once and the tables are let go. Canceling a pending table
     * after the call is sent has no other effect on it: a table that already exists on the server stays usable there,
     * and the call cannot be withdrawn.
     */
    public static <T> Promise<T> holdUntil(WorkerConnection connection, Ticket[] tickets, Supplier<Promise<T>> call) {
        Object retainer = new Object();
        List<ClientTableState> held = new ArrayList<>();
        for (Ticket ticket : tickets) {
            connection.getCache().get(new TableTicket(ticket)).ifPresent(state -> {
                if (!held.contains(state)) {
                    state.retain(retainer);
                    held.add(state);
                }
            });
        }
        LazyPromise<T> result = new LazyPromise<>();
        boolean[] lettingGo = {false};
        Runnable letGo = () -> {
            if (!lettingGo[0]) {
                lettingGo[0] = true;
                held.forEach(state -> state.unretain(retainer));
            }
        };
        for (ClientTableState state : held) {
            state.onFailedOrReleased(failure -> {
                letGo.run();
                result.fail("Operand table failed: " + failure);
            }, () -> {
                letGo.run();
                result.fail("Operand table was released");
            });
        }
        if (!lettingGo[0]) {
            // Nothing failed while being held, so the request names only tables the server has or will have
            Promise<T> sent;
            try {
                sent = call.get();
            } catch (RuntimeException e) {
                // Thrown while making the request: nothing will settle, so let go before passing it on
                letGo.run();
                throw e;
            }
            sent.then(value -> {
                letGo.run();
                result.succeed(value);
                return null;
            }, err -> {
                letGo.run();
                result.fail(err);
                return null;
            });
        }
        return result.asPromise();
    }

    /**
     * Why a request on {@code connection} must not name {@code operands}, or {@code null} if all are on it: a ticket
     * from another connection can name a different table on this one.
     */
    public static @Nullable String foreignOperand(WorkerConnection connection, JsTableReference... operands) {
        for (JsTableReference operand : operands) {
            if (operand.getConnection() != connection) {
                return "Operand table is not from the same connection as the table it is used with";
            }
        }
        return null;
    }

    private static @Nullable Pending<?> asPending(JsTableReference operand) {
        Object object = operand;
        return object instanceof Pending ? (Pending<?>) object : null;
    }

    /**
     * Has the {@code dependent} state hold {@code operand} until it settles, and fails it through {@code fail} if the
     * operand fails first. A table or resolved table contributes its current state, which its owner may otherwise
     * release before the dependent's request is sent.
     */
    private static void holdOperand(WorkerConnection connection, JsTableReference operand, ClientTableState dependent,
            Consumer<String> fail) {
        Pending<?> pending = asPending(operand);
        if (pending != null) {
            pending.retainUntilSettled(dependent, fail, "Operand table");
        } else {
            connection.getCache().get(new TableTicket(operand.typedTicket().getTicket()))
                    .ifPresent(state -> retainUntilSettled(state, dependent, fail, "Operand table"));
        }
    }

    /**
     * Queues an operation whose source state exists; the new state retains it until it has been created or has failed,
     * so the source is not released out from under it.
     */
    private static JsPendingTable<JsResolvedTable> enqueue(WorkerConnection connection, Ticket resultId,
            BatchTableRequest.Operation.Builder operation, ClientTableState sourceState,
            @Nullable JsTableReference operand) {
        return enqueue(connection, resultId, operation,
                dependent -> retainUntilSettled(sourceState, dependent.state, dependent::cancelIfUnsettled,
                        "Source table"),
                operand);
    }

    /**
     * Queues an operation on the given connection's next batch. {@code retainSource} is called with the new operation
     * before this returns, so the source is retained before anything else can run. The {@code operand}, if any, is held
     * the same way, so a caller may close it once the operation is created; a pending one is checked first.
     */
    private static JsPendingTable<JsResolvedTable> enqueue(WorkerConnection connection, Ticket resultId,
            BatchTableRequest.Operation.Builder operation, Consumer<Operation> retainSource,
            @Nullable JsTableReference operand) {
        if (operand != null && operand.getConnection() != connection) {
            throw new IllegalArgumentException("Operand table is not from the same connection as the source table");
        }
        if (operand != null) {
            String failure = operandFailure(operand);
            if (failure != null) {
                return failed(connection, resultId, operation, failure);
            }
        }
        Operation pending = new Operation(connection, new TableTicket(resultId), operation);
        retainSource.accept(pending);
        if (operand != null) {
            holdOperand(connection, operand, pending.state, pending::cancelIfUnsettled);
        }

        Batch batch = enqueued.get(connection);
        if (batch == null) {
            batch = new Batch();
            enqueued.put(connection, batch);
            Batch flushing = batch;

            // Wait a microtask, then send all accumulated requests in a single gRPC call, once the server is ready
            LazyPromise.runLater(() -> {
                Batch removed = enqueued.remove(connection);
                assert removed == flushing;
                sendBatch(connection, flushing);
            });
        }
        batch.ops.add(pending);
        return pending;
    }

    /**
     * An operation that has already failed with {@code message}. It is never sent, so nothing on the server references
     * a ticket that may never be defined, and anything chained on it or given it as an operand fails the same way.
     */
    private static Operation failed(WorkerConnection connection, Ticket resultId,
            BatchTableRequest.Operation.Builder operation, String message) {
        Operation failed = new Operation(connection, new TableTicket(resultId), operation);
        failed.failedAtBirth = true;
        failed.cancel(message);
        return failed;
    }

    /**
     * Delivers {@code owner}, which already retains {@code state}, once the state is running. If the state fails or is
     * released first, the owner is closed and the promise rejects.
     */
    private static LazyPromise<JsResolvedTable> whenRunning(Resolved owner, ClientTableState state, String what) {
        LazyPromise<JsResolvedTable> promise = new LazyPromise<>();
        state.onRunning(cts -> promise.succeed(owner), failure -> {
            owner.close();
            promise.fail(failure);
        }, () -> {
            owner.close();
            promise.fail(releasedBefore(what, "created"));
        });
        return promise;
    }

    /**
     * Why a pending table described as {@code what} was found released at the {@code step} it was used for. Nothing in
     * the task that creates it releases it, so this is a late use the task check did not catch.
     */
    private static String releasedBefore(String what, String step) {
        return "A pending table (" + what + ") was released before it was " + step
                + ": await it or chain on it in the task that creates it";
    }

    /**
     * Has the {@code dependent} state retain {@code source} until the dependent is running, failed, or released, and
     * fails the dependent through {@code fail} if the source fails first. A dependent that has already settled retains
     * nothing. {@code role} names the source in that failure, "Source table" or "Operand table".
     */
    private static void retainUntilSettled(ClientTableState source, ClientTableState state, Consumer<String> fail,
            String role) {
        if (state.getResolution() != ClientTableState.ResolutionState.UNRESOLVED) {
            return;
        }
        source.retain(state);
        state.onRunning(
                ignore -> source.unretain(state),
                failure -> source.unretain(state),
                () -> source.unretain(state));
        // A source that fails may never have been defined on the server (a fetch that failed before it was sent),
        // in which case the dependent would wait on a placeholder export forever; failing it here also releases its
        // ticket so the server drops that placeholder. When the server did define the source, this merely beats its
        // own dependency failure to the same result.
        source.onFailedOrReleased(
                failure -> fail.accept(role + " failed: " + failure),
                () -> fail.accept(role + " was released"));
    }

    private static void sendBatch(WorkerConnection connection, Batch batch) {
        connection.whenServerReady("batch table operations").then(ready -> {
            // Operations canceled before this point, including while waiting for the server, are never sent
            BatchTableRequest.Builder request = BatchTableRequest.newBuilder();
            for (Operation task : batch.ops) {
                if (task.cancelled()) {
                    continue;
                }
                task.sent = true;
                task.sentInSession = connection.getSessions();
                task.batch = batch;
                request.addOps(task.operation);
                batch.outstanding.put(task.ticket, task);
            }
            if (batch.outstanding.isEmpty()) {
                return null;
            }

            // Create a single stream for all operations, notify each individual operation of the result when it
            // finishes
            ResponseStreamWrapper<ExportedTableCreationResponse> stream = ResponseStreamWrapper.of(observer -> {
                connection.tableServiceClient().batch(request.build(), observer);
            });
            batch.stream = stream;
            stream.onData(etcr -> {
                Operation task = batch.outstanding.get(etcr.getResultId().getTicket());
                if (task == null) {
                    // A canceled operation's response; its state already failed and its ticket is being released
                    JsLog.debug("Batch response for a ticket no longer waited on", etcr.getResultId());
                    return;
                }
                if (etcr.getSuccess()) {
                    task.state.applyTableCreationResponse(etcr);
                } else {
                    task.fail(etcr.getErrorInfo());
                }
                // If nothing retains the state (never awaited), this releases it
                connection.scheduleCheck(task.state);
                batch.settled(task);
            });
            stream.onEnd(status -> {
                // Operations still waiting here were cut off by a stream failure; a stream the client closed has none
                for (Operation task : batch.outstanding.values()) {
                    task.fail(String.valueOf(status.getDescription()));
                    connection.scheduleCheck(task.state);
                }
                batch.outstanding.clear();
                batch.stream = null;
            });
            return null;
        }).catch_(err -> {
            // The server was not ready, or setting up the stream threw: nothing else observes this, so the states
            // must carry the failure
            for (Operation task : batch.ops) {
                if (task.cancelled()) {
                    continue;
                }
                task.fail(String.valueOf(err));
                connection.scheduleCheck(task.state);
            }
            batch.outstanding.clear();
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
