package io.deephaven.client.impl;

import static io.deephaven.client.impl.BatchTableRequestBuilder.longToByteString;

import io.deephaven.client.ExportManager;
import io.deephaven.client.ExportedTable;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.table.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

abstract class ExportManagerImpl implements ExportManager {

    private final Set<ExportedTableImpl> exports;
    private long nextTicket;

    public ExportManagerImpl() {
        this.exports = new HashSet<>();
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
        List<ExportedTableImpl> newExports = new ArrayList<>(tables.size());
        for (Table table : tables) {

            final Optional<ExportedTableImpl> existing = lookup(table);
            if (existing.isPresent()) {
                results.add(existing.get());
                continue;
            }

            final ExportedTableImpl newExport = ImmutableExportedTableImpl.builder().manager(this)
                .table(table).ticket(nextTicket++).build();

            newExports.add(newExport);
            results.add(newExport);
        }
        if (newExports.isEmpty()) {
            return results;
        }

        final BatchTableRequest request = BatchTableRequestBuilder.build(newExports);

        execute(request); // todo: handle async success / failure

        exports.addAll(newExports);

        return results;
    }

    @Override
    public void releaseAll() {
        Set<ExportedTableImpl> toRelease = new LinkedHashSet<>(exports);
        exports.clear();
        releaseImpls(toRelease);
    }

    @Override
    public synchronized void release(ExportedTable export) {
        release(Collections.singleton(export));
    }

    @Override
    public synchronized void release(Collection<ExportedTable> exports) {
        Set<ExportedTableImpl> toRelease = new LinkedHashSet<>(exports.size());
        for (ExportedTable export : exports) {
            if (!(export instanceof ExportedTableImpl)) {
                throw new IllegalArgumentException();
            }
            final ExportedTableImpl impl = (ExportedTableImpl) export;
            if (!equals(impl.manager())) {
                throw new IllegalArgumentException();
            }
            if (!this.exports.contains(impl)) {
                continue;
            }
            toRelease.add(impl);
        }
        for (ExportedTableImpl exportedTable : toRelease) {
            this.exports.remove(exportedTable);
        }
        releaseImpls(toRelease);
    }

    private void releaseImpls(Set<ExportedTableImpl> impls) {
        // TODO: batch releases
        for (ExportedTableImpl impl : impls) {
            executeRelease(Ticket.newBuilder().setId(longToByteString(impl.ticket())).build());
        }
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
