package io.deephaven.engine.v2.select.python;

import io.deephaven.engine.v2.select.Formula.FillContext;

enum FillContextPython implements FillContext {
    EMPTY;

    @Override
    public void close() {
        // ignore
    }
}
