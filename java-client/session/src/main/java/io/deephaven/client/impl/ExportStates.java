//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import com.google.common.annotations.VisibleForTesting;
import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.ReleaseResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.TableSpec;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class ExportStates implements ExportService {

    private final SessionImpl session;
    private final SessionServiceStub sessionStub;
    private final TableServiceStub tableStub;

    private final Map<TableSpec, State> exports;
    private final ExportTicketCreator exportTicketCreator;
    private final Lock lock;

    ExportStates(SessionImpl session, SessionServiceStub sessionStub, TableServiceStub tableStub,
            ExportTicketCreator exportTicketCreator) {
        this.session = Objects.requireNonNull(session);
        this.sessionStub = Objects.requireNonNull(sessionStub);
        this.tableStub = Objects.requireNonNull(tableStub);
        this.exportTicketCreator = Objects.requireNonNull(exportTicketCreator);
        this.exports = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    @VisibleForTesting
    ExportStates(SessionServiceStub sessionStub, TableServiceStub tableStub, ExportTicketCreator exportTicketCreator) {
        this.session = null;
        this.sessionStub = Objects.requireNonNull(sessionStub);
        this.tableStub = Objects.requireNonNull(tableStub);
        this.exportTicketCreator = Objects.requireNonNull(exportTicketCreator);
        this.exports = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    /**
     * An unreferenceable table is a table that is reachable from the current set of exports, but isn't an export
     * itself. An unreferenceable table can no longer be referenced by the client.
     */
    private Set<TableSpec> unreferenceableTables() {
        // todo: potentially keep around Set<TableSpec> unreferenceableTables as class member
        final Set<TableSpec> unreferenceableTables = ParentsVisitor.reachable(exports.keySet());
        unreferenceableTables.removeAll(exports.keySet());
        return unreferenceableTables;
    }

    private Optional<TableSpec> searchUnreferenceableTable(ExportsRequest request) {
        final Set<TableSpec> unreferenceableTables = unreferenceableTables();
        final Set<TableSpec> keySet = exports.keySet();
        // Note: this is *not* excluding everything that can be reached via exports.keySet(), it
        // just excludes paths from the request roots that go through an export.keySet().
        return ParentsVisitor.search(request.tables(), keySet::contains,
                unreferenceableTables::contains);
    }

    @Override
    public ExportServiceRequest exportRequest(ExportsRequest requests) {
        lock.lock();
        try {
            return exportRequestImpl(requests);
        } catch (Throwable t) {
            lock.unlock();
            throw t;
        }
    }

    private ExportServiceRequest exportRequestImpl(ExportsRequest requests) {
        ensureNoUnreferenceableTables(requests);

        final Set<TableSpec> oldExports = new HashSet<>(exports.keySet());

        final List<Export> results = new ArrayList<>(requests.size());
        final Set<TableSpec> newSpecs = new HashSet<>(requests.size());
        // linked so TableCreationHandler has a definitive order
        final Map<Integer, State> newStates = new LinkedHashMap<>(requests.size());

        for (ExportRequest request : requests) {
            final Optional<State> existing = lookup(request.table());
            if (existing.isPresent()) {
                final State existingState = existing.get();
                final Export newReference = existingState.newReference(request.listener());
                results.add(newReference);
                continue;
            }

            final int exportId = exportTicketCreator.createExportId();
            final State state = new State(request.table(), exportId);
            if (exports.putIfAbsent(request.table(), state) != null) {
                throw new IllegalStateException("Unable to put export, already exists");
            }

            final Export newExport = state.newReference(request.listener());
            newSpecs.add(request.table());
            newStates.put(exportId, state);
            results.add(newExport);
        }
        final Runnable send;
        if (!newSpecs.isEmpty()) {
            final List<TableSpec> postOrder = postOrderNewDependencies(oldExports, newSpecs);
            if (postOrder.isEmpty()) {
                throw new IllegalStateException();
            }
            final BatchTableRequest request =
                    BatchTableRequestBuilder.buildNoChecks(this::lookupTicket, postOrder);
            if (request.getOpsCount() == 0) {
                throw new IllegalStateException();
            }
            final BatchHandler batchHandler = new BatchHandler(newStates);
            send = () -> tableStub.batch(request, batchHandler);
        } else {
            send = () -> {
            };
        }
        return new ExportServiceRequest() {
            boolean sent;
            boolean closed;

            @Override
            public List<Export> exports() {
                return results;
            }

            @Override
            public void send() {
                if (closed || sent) {
                    return;
                }
                sent = true;
                // After the user has called send, all handling of state needs to be handled by the respective
                // io.deephaven.client.impl.ExportRequest.listener
                send.run();

            }

            @Override
            public void close() {
                if (closed) {
                    return;
                }
                closed = true;
                try {
                    if (!sent) {
                        cleanupUnsent();
                    }
                } finally {
                    lock.unlock();
                }
            }

            private void cleanupUnsent() {
                for (Export result : results) {
                    final State state = result.state();
                    if (newStates.containsKey(state.exportId())) {
                        // On brand new states that we didn't even send, we can simply remove them. We aren't
                        // leaking anything, but we have incremented our export id creator state.
                        removeImpl(state);
                        continue;
                    }
                    result.release();
                }
            }
        };
    }


    private void remove(State state) {
        lock.lock();
        try {
            removeImpl(state);
        } finally {
            lock.unlock();
        }
    }

    private void removeImpl(State state) {
        if (!exports.remove(state.table(), state)) {
            throw new IllegalStateException("Unable to remove state");
        }
    }

    private Optional<State> lookup(TableSpec table) {
        return Optional.ofNullable(exports.get(table));
    }

    private OptionalInt lookupTicket(TableSpec table) {
        final Optional<State> state = lookup(table);
        return state.isPresent() ? OptionalInt.of(state.get().exportId()) : OptionalInt.empty();
    }

    private static List<TableSpec> postOrderNewDependencies(Set<TableSpec> oldExports,
            Set<TableSpec> newExports) {
        Set<TableSpec> reachableOld = ParentsVisitor.reachable(oldExports);
        List<TableSpec> postOrderNew = ParentsVisitor.postOrderList(newExports);
        List<TableSpec> postOrderNewExcludeOld = new ArrayList<>(postOrderNew.size());
        for (TableSpec table : postOrderNew) {
            if (!reachableOld.contains(table)) {
                postOrderNewExcludeOld.add(table);
            }
        }
        return postOrderNewExcludeOld;
    }

    private void ensureNoUnreferenceableTables(ExportsRequest requests) {
        final Optional<TableSpec> unreferenceable = searchUnreferenceableTable(requests);
        if (unreferenceable.isPresent()) {
            // TODO(deephaven-core#4733): Add RPC to export a Table's parent(s)
            // Alternatively, we could have an implementation that exports everything along the chain.
            throw new IllegalArgumentException(String.format(
                    "Unable to complete request, contains an unreferenceable table: %s. This is an indication that the"
                            + " query is trying to export a strict sub-DAG of the existing exports; this is problematic"
                            + " because there isn't (currently) a way to construct a query that guarantees the returned"
                            + " export would refer to the same physical table that the existing exports are based on."
                            + " See https://github.com/deephaven/deephaven-core/issues/4733 for future improvements in"
                            + " this regard.",
                    unreferenceable.get()));
        }
    }

    class State {

        private final TableSpec table;
        private final int exportId;

        private final Set<Export> children;
        private ExportedTableCreationResponse creationResponse;
        private Throwable creationThrowable;
        private boolean creationCompleted;

        private boolean released;

        State(TableSpec table, int exportId) {
            this.table = Objects.requireNonNull(table);
            this.exportId = exportId;
            this.children = new LinkedHashSet<>();
        }

        Session session() {
            return session;
        }

        TableSpec table() {
            return table;
        }

        int exportId() {
            return exportId;
        }

        ExportStates exportStates() {
            return ExportStates.this;
        }

        synchronized Export newReference(Listener listener) {
            if (released) {
                throw new IllegalStateException(
                        "Should not be creating new references from state after the state has been released");
            }
            Export export = new Export(this, listener);
            addChild(export);
            return export;
        }

        synchronized void release(Export export) {
            if (!children.remove(export)) {
                throw new IllegalStateException("Unable to remove child");
            }
            if (children.isEmpty()) {
                ExportStates.this.remove(this);
                released = true;
                sessionStub.release(
                        ReleaseRequest.newBuilder().setId(ExportTicketHelper.wrapExportIdInTicket(exportId)).build(),
                        new TicketReleaseHandler(exportId));
            }
        }

        synchronized void onCreationResponse(ExportedTableCreationResponse creationResponse) {
            if (this.creationResponse != null) {
                throw new IllegalStateException("Only expected at most one creation response");
            }
            this.creationResponse = Objects.requireNonNull(creationResponse);
            for (Export child : children) {
                child.listener().onNext(creationResponse);
            }
        }

        synchronized void onCreationError(Throwable t) {
            if (this.creationThrowable != null) {
                throw new IllegalStateException("Only expected at most one creation throwable");
            }
            this.creationThrowable = Objects.requireNonNull(t);
            for (Export child : children) {
                child.listener().onError(t);
            }
        }

        synchronized void onCreationCompleted() {
            if (this.creationCompleted) {
                throw new IllegalStateException("Only expected at most one creation completed");
            }
            this.creationCompleted = true;
            for (Export child : children) {
                child.listener().onCompleted();
            }
        }

        private void addChild(Export export) {
            if (!children.add(export)) {
                throw new IllegalStateException("Unable to add child");
            }
            if (creationResponse != null) {
                export.listener().onNext(creationResponse);
            }
            if (creationThrowable != null) {
                export.listener().onError(creationThrowable);
            }
            if (creationCompleted) {
                export.listener().onCompleted();
            }
        }
    }

    private static final class TicketReleaseHandler implements StreamObserver<ReleaseResponse> {

        private static final Logger log = LoggerFactory.getLogger(TicketReleaseHandler.class);

        private final int exportId;

        private TicketReleaseHandler(int exportId) {
            this.exportId = exportId;
        }

        @Override
        public void onNext(ReleaseResponse value) {
            // success!
        }

        @Override
        public void onError(Throwable t) {
            log.error(String.format("onError releasing export id %d%n", exportId), t);
        }

        @Override
        public void onCompleted() {

        }
    }

    private static final class BatchHandler
            implements StreamObserver<ExportedTableCreationResponse> {

        private static final Logger log = LoggerFactory.getLogger(BatchHandler.class);

        private final Map<Integer, State> newStates;
        private final Set<State> handled;

        private BatchHandler(Map<Integer, State> newStates) {
            this.newStates = Objects.requireNonNull(newStates);
            this.handled = new HashSet<>(newStates.size());
        }

        @Override
        public void onNext(ExportedTableCreationResponse value) {
            if (!value.getResultId().hasTicket()) {
                // Not currently passing through responses for non-exported operations.
                // Errors in non-exported operations will trigger appropriate responses for exported
                // operations that
                // depend on them.
                return;
            }
            if (Ticket.getDefaultInstance().equals(value.getResultId().getTicket())) {
                throw new IllegalStateException(
                        "Not expecting export creation responses for empty tickets");
            }
            final int exportId = ExportTicketHelper.ticketToExportId(value.getResultId().getTicket(), "export");
            final State state = newStates.get(exportId);
            if (state == null) {
                throw new IllegalStateException("Unable to find state for creation response");
            }
            if (!handled.add(state)) {
                throw new IllegalStateException(
                        String.format("Server misbehaving, already received response for export id %d", exportId));
            }
            try {
                state.onCreationResponse(value);
            } catch (RuntimeException e) {
                log.error("state.onCreationResponse had unexpected exception", e);
                state.onCreationError(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            for (State state : newStates.values()) {
                try {
                    state.onCreationError(t);
                } catch (RuntimeException e) {
                    log.error("state.onCreationError had unexpected exception, ignoring", e);
                }
            }
        }

        @Override
        public void onCompleted() {
            for (State state : newStates.values()) {
                try {
                    state.onCreationCompleted();
                } catch (RuntimeException e) {
                    log.error("state.onCreationCompleted had unexpected exception, ignoring", e);
                }
            }
        }
    }
}
