/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.pool;

import java.util.LinkedList;
import java.util.List;

import io.deephaven.base.Function;
import io.deephaven.base.testing.RecordingMockObject;
import io.deephaven.base.verify.Assert;

// --------------------------------------------------------------------
/**
 * Mock factory
 */
public class MockFactory<T> extends RecordingMockObject implements Function.Nullary<T> {

    private final List<T> m_items = new LinkedList<T>();

    // ------------------------------------------------------------
    public void add(T t) {
        m_items.add(t);
    }

    // ------------------------------------------------------------
    @Override // from Function.Nullary
    public T call() {
        recordActivity("call()");
        Assert.eqFalse(m_items.isEmpty(), "m_items.isEmpty()");
        return m_items.remove(0);
    }
}
