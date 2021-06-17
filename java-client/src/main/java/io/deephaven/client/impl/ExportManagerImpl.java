package io.deephaven.client.impl;

import io.deephaven.client.ExportManager;
import io.deephaven.client.ExportedTable;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.table.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

abstract class ExportManagerImpl implements ExportManager {

    class State {

        private final Table table;
        private final long ticket;

        private int localRefs; // note: all release ops go through the ExportManager which will
                               // guard this

        State(Table table, long ticket) {
            this.table = Objects.requireNonNull(table);
            this.ticket = ticket;
            this.localRefs = 1;
            exports.put(table, this);
        }

        ExportManagerImpl manager() {
            return ExportManagerImpl.this;
        }

        Table table() {
            return table;
        }

        long ticket() {
            return ticket;
        }

        void incRef() {
            synchronized (manager()) {
                if (localRefs <= 0) {
                    throw new IllegalStateException();
                }
                localRefs += 1;
            }
        }

        void decRef() {
            synchronized (manager()) {
                if (localRefs <= 0) {
                    throw new IllegalStateException();
                }
                localRefs -= 1;
                if (localRefs == 0) {
                    exports.remove(table);
                }
            }
        }

        ExportedTableImpl newRef() {
            incRef();
            return new ExportedTableImpl(this);
        }
    }

    private final Map<Table, State> exports;
    private long nextTicket;

    public ExportManagerImpl() {
        this.exports = new HashMap<>();
        this.nextTicket = 1L;
    }

    protected abstract void execute(BatchTableRequest batchTableRequest);

    protected abstract void executeRelease(Ticket ticket);

    @Override
    public synchronized ExportedTable export(Table table) {
        return export(Collections.singleton(table)).get(0);
    }

    @Override
    public synchronized List<ExportedTable> export(Collection<Table> tables) {
        List<ExportedTable> results = new ArrayList<>(tables.size());
        List<State> newStates = new ArrayList<>(tables.size());
        for (Table table : tables) {

            final Optional<State> existing = lookup(table);
            if (existing.isPresent()) {
                final ExportedTable newRef = existing.get().newRef();
                results.add(newRef);
                continue;
            }

            final long ticket = nextTicket++;
            final State state = new State(table, ticket);
            final ExportedTableImpl newExport = new ExportedTableImpl(state);
            newStates.add(state);
            results.add(newExport);
        }
        if (newStates.isEmpty()) {
            return results;
        }

        final BatchTableRequest request = BatchTableRequestBuilder.build(newStates);

        execute(request); // todo: handle async success / failure

        for (State newState : newStates) {
            exports.put(newState.table(), newState);
        }

        return results;
    }

    private Optional<State> lookup(Table table) {
        return Optional.ofNullable(exports.get(table));
    }
}
