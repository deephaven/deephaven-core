//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.pool;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import io.deephaven.base.testing.RecordingMockObject;
import io.deephaven.base.testing.Thumbprinter;
import io.deephaven.base.verify.Assert;

// --------------------------------------------------------------------
/**
 * Mock object for {@link io.deephaven.base.pool.Pool}.
 */
public class MockPool<T> extends RecordingMockObject implements Pool<T> {

    private final List<T> m_items = new LinkedList<T>();
    private Consumer<T> m_clearingProcedure;
    private Thumbprinter<T> m_thumbprinter = Thumbprinter.DEFAULT;

    // ----------------------------------------------------------------
    public void setClearingProcedure(Consumer<T> clearingProcedure) {
        m_clearingProcedure = clearingProcedure;
    }

    // ----------------------------------------------------------------
    public void setThumbprinter(Thumbprinter<T> thumbprinter) {
        m_thumbprinter = thumbprinter;
    }

    // ----------------------------------------------------------------
    public MockPool<T> addItem(T t) {
        m_items.add(t);
        return this;
    }

    // ----------------------------------------------------------------
    public void assertIsEmpty() {
        junit.framework.Assert.assertTrue("all items taken", m_items.isEmpty());
    }

    // ----------------------------------------------------------------
    @Override // from Pool
    public T take() {
        recordActivity("take()");
        Assert.eqFalse(m_items.isEmpty(), "m_items.isEmpty()");
        return m_items.remove(0);
    }

    // ----------------------------------------------------------------
    @Override // from Pool
    public void give(T item) {
        if (null != m_clearingProcedure) {
            m_clearingProcedure.accept(item);
        }
        recordActivity("give(" + m_thumbprinter.getThumbprint(item) + ")");
    }
}
