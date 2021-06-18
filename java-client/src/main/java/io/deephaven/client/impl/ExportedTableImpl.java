package io.deephaven.client.impl;

import io.deephaven.qst.manager.ExportedTable;
import io.deephaven.client.impl.ExportManagerImpl.State;
import io.deephaven.qst.table.Table;
import java.util.Objects;

class ExportedTableImpl implements ExportedTable {

    private final ExportManagerImpl.State state;
    private boolean released;

    ExportedTableImpl(ExportManagerImpl.State state) {
        this.state = Objects.requireNonNull(state);
        this.released = false;
    }

    final State state() {
        return state;
    }

    @Override
    public final Table table() {
        return state.table();
    }

    synchronized final boolean isReleased() {
        return released;
    }

    @Override
    public synchronized final ExportedTable newRef() {
        if (released) {
            throw new IllegalStateException("Should not take newRef after release");
        }
        return state.newRef();
    }

    public final long ticket() {
        return state.ticket();
    }

    @Override
    public synchronized final void release() {
        if (released) {
            return;
        }
        state.decRef();
        released = true;
    }
}
