//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base;

public interface Copyable<T> extends SafeCloneable<T> {
    void copyValues(T other);
}
