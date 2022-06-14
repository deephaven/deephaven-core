/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.engine.table.impl.select.Formula.FillContext;

enum FillContextPython implements FillContext {
    EMPTY;

    @Override
    public void close() {
        // ignore
    }
}
