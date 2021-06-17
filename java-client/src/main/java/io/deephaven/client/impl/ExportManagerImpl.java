package io.deephaven.client.impl;

import io.deephaven.client.ExportManager;
import io.deephaven.client.ExportedTable;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.qst.table.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class ExportManagerImpl implements ExportManager {

    private final Set<ExportedTableImpl> exports;
    private long nextTicket;

    public ExportManagerImpl(Set<ExportedTableImpl> exports) {
        this.exports = exports;
    }

    @Override
    public synchronized ExportedTable export(Table table) {
        return export(Collections.singletonList(table)).get(0);
    }

    @Override
    public synchronized List<ExportedTable> export(Collection<Table> tables) {
        List<ExportedTable> results = new ArrayList<>(tables.size());
        List<ExportedTableImpl> newTables = new ArrayList<>(tables.size());
        for (Table table : tables) {

            final Optional<ExportedTableImpl> existing = lookup(table);
            if (existing.isPresent()) {
                results.add(existing.get());
                continue;
            }

            final ExportedTableImpl newExport = ImmutableExportedTableImpl.builder().manager(this)
                .table(table).ticket(nextTicket++).build();

            newTables.add(newExport);
            results.add(newExport);
        }
        if (newTables.isEmpty()) {
            return results;
        }

        final BatchTableRequest request = BatchTableRequestBuilder.build(newTables);

        // TODO execute

        return results;
    }

    @Override
    public void releaseAll() {
        final Iterator<ExportedTableImpl> it = exports.iterator();
        while (it.hasNext()) {
            releaseImpl(it.next());
            it.remove();
        }
    }

    @Override
    public synchronized void release(ExportedTable export) {
        if (!(export instanceof ExportedTableImpl)) {
            throw new IllegalArgumentException();
        }
        final ExportedTableImpl impl = (ExportedTableImpl) export;
        if (!equals(impl.manager())) {
            throw new IllegalArgumentException();
        }
        if (!exports.contains(impl)) {
            return;
        }

        releaseImpl(impl);

        exports.remove(impl);
    }

    @Override
    public synchronized void release(Collection<ExportedTable> exports) {
        // todo batch release requests

        for (ExportedTable export : exports) {
            release(export);
        }
    }

    private void releaseImpl(ExportedTableImpl impl) {
        // TODO release
    }

    private Optional<ExportedTableImpl> lookup(Table table) {
        // note: we could speed this up w/ Map if necessary
        for (ExportedTableImpl export : exports) {
            if (table.equals(export.table())) {
                return Optional.of(export);
            }
        }
        return Optional.empty();
    }
}
