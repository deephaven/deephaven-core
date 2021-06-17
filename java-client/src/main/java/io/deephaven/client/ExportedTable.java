package io.deephaven.client;

import io.deephaven.qst.table.Table;
import java.io.Closeable;

public interface ExportedTable extends Closeable {

    Table table();

    ExportedTable newRef();

    void release();

    @Override
    default void close() {
        release();
    }
}
