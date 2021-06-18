package io.deephaven.client;

import io.deephaven.qst.table.Table;
import java.io.Closeable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Keeps exported references to a set of {@link Table Tables}.
 */
public class ExportedTables implements Closeable {

    private static final Collector<ExportedTable, ?, Map<Table, ExportedTable>> MAP_COLLECTOR =
        Collectors.toMap(ExportedTable::table, Function.identity());

    public static ExportedTables of(ExportManager manager, Collection<Table> tables) {
        // it doesn't make sense for us to duplicate any references in this context
        final Set<Table> set = toSet(tables);
        final Map<Table, ExportedTable> exports =
            manager.export(set).stream().collect(MAP_COLLECTOR);
        return new ExportedTables(exports);
    }

    private static Set<Table> toSet(Collection<Table> collection) {
        if (collection instanceof Set) {
            return (Set<Table>) collection;
        }
        return new LinkedHashSet<>(collection);
    }

    private final Map<Table, ExportedTable> exports;
    private boolean released;

    private ExportedTables(Map<Table, ExportedTable> exports) {
        this.exports = Objects.requireNonNull(exports);
        this.released = false;
    }

    public synchronized boolean isReleased() {
        return released;
    }

    public synchronized ExportedTable newRef(Table table) {
        if (released) {
            throw new IllegalStateException("Should not take newRef after release");
        }
        final ExportedTable exportedTable = exports.get(table);
        if (exportedTable == null) {
            throw new IllegalStateException("Table not under management");
        }
        return exportedTable.newRef();
    }

    public synchronized ExportedTables newRefs() {
        if (released) {
            throw new IllegalStateException("Should not take newRefs after release");
        }
        final Map<Table, ExportedTable> newRefs =
            exports.values().stream().map(ExportedTable::newRef).collect(MAP_COLLECTOR);
        return new ExportedTables(newRefs);
    }

    public synchronized final void release() {
        if (released) {
            return;
        }
        for (ExportedTable table : exports.values()) {
            table.release();
        }
        released = true;
    }

    @Override
    public final void close() {
        release();
    }
}
