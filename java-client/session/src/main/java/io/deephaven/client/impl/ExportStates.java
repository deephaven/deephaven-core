package io.deephaven.client.impl;

import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ReleaseResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.grpc.Ticket;
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
import java.util.Set;

final class ExportStates {

    private static final Logger log = LoggerFactory.getLogger(ExportStates.class);

    private final SessionServiceStub sessionStub;
    private final TableServiceStub tableStub;

    private final Map<TableSpec, State> exports;
    private int nextTicket;

    ExportStates(SessionServiceStub sessionStub, TableServiceStub tableStub) {
        this.sessionStub = Objects.requireNonNull(sessionStub);
        this.tableStub = Objects.requireNonNull(tableStub);
        this.exports = new HashMap<>();
        this.nextTicket = 1;
    }

    synchronized List<Export> export(ExportsRequest requests) {
        final List<Export> results = new ArrayList<>(requests.size());
        final Set<TableSpec> newSpecs = new HashSet<>(requests.size());
        // linked so TableCreationHandler has a definitive order
        final Map<Ticket, State> newStates = new LinkedHashMap<>(requests.size());

        for (ExportRequest request : requests) {
            final Optional<State> existing = lookup(request.table());
            if (existing.isPresent()) {
                final State existingState = existing.get();
                final Export newReference = existingState.newReference(request.listener());
                results.add(newReference);
                continue;
            }

            final Ticket ticket = ExportTicketHelper.exportIdToTicket(nextTicket++);
            final State state = new State(request.table(), ticket);
            if (exports.putIfAbsent(request.table(), state) != null) {
                throw new IllegalStateException("Unable to put export, already exists");
            }

            final Export newExport = state.newReference(request.listener());
            newSpecs.add(request.table());
            newStates.put(ticket, state);
            results.add(newExport);
        }

        if (!newSpecs.isEmpty()) {
            final BatchTableRequest request =
                BatchTableRequestBuilder.build(this::lookupTicket, newSpecs);
            log.debug("Sending batch: {}", request);
            tableStub.batch(request, new BatchHandler(newStates));
        }

        return results;
    }

    private synchronized void release(State state) {
        if (!exports.remove(state.table(), state)) {
            throw new IllegalStateException("Unable to remove state");
        }
    }

    private Optional<State> lookup(TableSpec table) {
        return Optional.ofNullable(exports.get(table));
    }

    private Optional<Ticket> lookupTicket(TableSpec table) {
        return lookup(table).map(State::ticket);
    }

    class State {

        private final TableSpec table;
        private final Ticket ticket;

        private final Set<Export> children;
        private ExportedTableCreationResponse creationResponse;
        private Throwable creationThrowable;
        private boolean creationCompleted;

        private boolean released;

        State(TableSpec table, Ticket ticket) {
            this.table = Objects.requireNonNull(table);
            this.ticket = Objects.requireNonNull(ticket);
            this.children = new LinkedHashSet<>();
        }

        TableSpec table() {
            return table;
        }

        Ticket ticket() {
            return ticket;
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
                ExportStates.this.release(this);
                released = true;
                sessionStub.release(ticket, new TicketReleaseHandler(ticket));
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

        private final Ticket ticket;

        private TicketReleaseHandler(Ticket ticket) {
            this.ticket = Objects.requireNonNull(ticket);
        }

        @Override
        public void onNext(ReleaseResponse value) {
            if (!value.getSuccess()) {
                log.warn("Unable to release ticket '{}'",
                    ExportTicketHelper.toReadableString(ticket));
            }
        }

        @Override
        public void onError(Throwable t) {
            log.error(String.format("onError releasing ticket '%s'",
                ExportTicketHelper.toReadableString(ticket)), t);
        }

        @Override
        public void onCompleted() {

        }
    }

    private static final class BatchHandler
        implements StreamObserver<ExportedTableCreationResponse> {

        private final Map<Ticket, State> newStates;

        private BatchHandler(Map<Ticket, State> newStates) {
            this.newStates = Objects.requireNonNull(newStates);
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
            final State state = newStates.remove(value.getResultId().getTicket());
            if (state == null) {
                throw new IllegalStateException("Unable to find state for creation response");
            }
            state.onCreationResponse(value);
        }

        @Override
        public void onError(Throwable t) {
            for (State state : newStates.values()) {
                state.onCreationError(t);
            }
        }

        @Override
        public void onCompleted() {
            for (State state : newStates.values()) {
                state.onCreationCompleted();
            }
        }
    }
}
