package io.deephaven.client;

import io.deephaven.qst.table.Table;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;

public interface ExportManager extends Closeable {

    ExportedTable export(Table table);

    List<ExportedTable> export(Collection<Table> tables);

    void releaseAll();

    void release(ExportedTable export);

    void release(Collection<ExportedTable> exports);

    @Override
    default void close() {
        releaseAll();
    }
}
