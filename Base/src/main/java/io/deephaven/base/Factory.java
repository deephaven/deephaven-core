//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base;

public interface Factory<T> {
    public T create();

    public static interface Unary<T, A> {
        public T create(A argument);
    }
}
