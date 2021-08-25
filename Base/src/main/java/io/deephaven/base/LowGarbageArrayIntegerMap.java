/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

// --------------------------------------------------------------------
/**
 * A very simple {@link Map} for small maps with Integer keys (uses direct array access) that creates no garbage (except
 * when expanding). This set only has one {@link Iterator}, which is reused. This set is not thread safe.
 * <P>
 * Note: This class extends {@link HashMap} rather than {@link Map} (or {@link AbstractMap}) only because one of the
 * fields where we want to use it ({@link sun.nio.ch.EPollSelectorImpl#fdToKey}) is (improperly) declared as a HashMap
 * rather than a Map.
 */
public class LowGarbageArrayIntegerMap<T> extends HashMap<Integer, T> {

    private Object[] m_values = new Object[500];

    @Override
    public T get(Object key) {
        if (key instanceof Integer) {
            int nIndex = (Integer) key;
            if (nIndex < 0 || nIndex >= m_values.length) {
                return null;
            }
            return (T) m_values[nIndex];
        }
        return null;
    }

    @Override
    public T put(Integer key, T value) {
        if (null != key) {
            Require.geqZero(key, "nIndex");
            if (key >= m_values.length) {
                m_values = ArrayUtil.extend(m_values, key, Object.class);
            }
            Object oldValue = m_values[key];
            m_values[key] = value;
            return (T) oldValue;
        }
        throw Require.valueNeverOccurs(key, "key");
    }

    @Override
    public T remove(Object key) {
        if (key instanceof Integer) {
            int nIndex = (Integer) key;
            if (nIndex < 0 || nIndex >= m_values.length) {
                return null;
            }
            Object oldValue = m_values[nIndex];
            m_values[nIndex] = null;
            return (T) oldValue;
        }
        return null;
    }

    // ################################################################
    // currently unsupported methods

    @Override
    public int size() {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public boolean isEmpty() {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public boolean containsKey(Object key) {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends T> m) {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public void clear() {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public boolean containsValue(Object value) {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public Object clone() {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public Set<Integer> keySet() {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public Collection<T> values() {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public Set<Map.Entry<Integer, T>> entrySet() {
        throw Assert.statementNeverExecuted();
    }
}
