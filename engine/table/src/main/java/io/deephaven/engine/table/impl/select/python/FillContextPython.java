package io.deephaven.engine.table.impl.select.python;

import io.deephaven.engine.table.impl.select.Formula.FillContext;

enum FillContextPython implements FillContext {
    EMPTY;

    @Override
    public void close() {
        // ignore
    }
}
