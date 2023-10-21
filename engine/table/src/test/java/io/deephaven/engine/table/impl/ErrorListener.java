/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;

public class ErrorListener extends InstrumentedTableUpdateListenerAdapter {

    private Throwable originalException;

    public ErrorListener(Table table) {
        super("Error Checker", table, false);
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {}

    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        this.originalException = originalException;
    }

    public Throwable originalException() {
        return originalException;
    }
}
