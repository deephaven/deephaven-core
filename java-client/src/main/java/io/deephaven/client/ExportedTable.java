package io.deephaven.client;

import io.deephaven.qst.table.Table;
import java.io.Closeable;

public interface ExportedTable extends Closeable {

    ExportManager manager();

    Table table();

    default void release() {
        manager().release(this);
    }

    @Override
    default void close() {
        release();
    }
}
