package io.deephaven.qst.manager;

import io.deephaven.qst.table.Table;
import java.util.Objects;

public abstract class ExportedTableBase<EXPORT extends ExportedTable> implements ExportedTable {
    private final Table table;
    private boolean released;

    protected ExportedTableBase(Table table) {
        this.table = Objects.requireNonNull(table);
        this.released = false;
    }

    /**
     * Guarded, guaranteed to be called synchronously and not released.
     */
    protected abstract void releaseImpl();

    /**
     * Guarded, guaranteed to be called synchronously and not released.
     */
    protected abstract EXPORT newRefImpl();

    @Override
    public final Table table() {
        return table;
    }

    @Override
    public final synchronized EXPORT newRef() {
        if (released) {
            throw new IllegalStateException("Should not take newRef after release");
        }
        return newRefImpl();
    }

    @Override
    public final synchronized void release() {
        if (released) {
            return;
        }
        releaseImpl();
        released = true;
    }
}
