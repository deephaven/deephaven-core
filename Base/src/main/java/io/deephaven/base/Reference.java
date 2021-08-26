/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

// --------------------------------------------------------------------
/**
 * Encapsulates an object reference so it can be passed and modified.
 */
public class Reference<T> {

    private T m_value;

    public Reference() {}

    public Reference(T value) {
        m_value = value;
    }

    public T getValue() {
        return m_value;
    }

    public void setValue(T value) {
        m_value = value;
    }
}
