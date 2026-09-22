//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.TableUpdate;

import static org.junit.Assert.*;

public class FailureListener extends InstrumentedTableUpdateListener {
    public FailureListener() {
        super("Fuzzer Failure Listener");
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {}

    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        originalException.printStackTrace();
        fail(originalException.getMessage());
    }
}
