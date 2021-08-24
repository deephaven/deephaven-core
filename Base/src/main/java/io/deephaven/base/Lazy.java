/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.util.function.Supplier;

public class Lazy<T> implements Supplier<T> {
    private Supplier<T> supplier;

    public Lazy(Supplier<T> x) {
        supplier = () -> {
            T val = x.get();
            supplier = () -> val;
            return val;
        };
    }

    public synchronized T get() {
        return supplier.get();
    }
};
