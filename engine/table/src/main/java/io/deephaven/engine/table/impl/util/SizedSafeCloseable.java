package io.deephaven.engine.table.impl.util;

import io.deephaven.util.SafeCloseable;

import java.util.function.IntFunction;

public final class SizedSafeCloseable<T extends SafeCloseable> implements SafeCloseable {
    private final IntFunction<T> supplier;
    private T closeable;
    private int capacity;

    public SizedSafeCloseable(IntFunction<T> supplier) {
        this.supplier = supplier;
    }

    public T get() {
        return closeable;
    }

    public T ensureCapacity(int capacity) {
        if (capacity > this.capacity) {
            if (closeable != null) {
                closeable.close();
            }
            closeable = supplier.apply(capacity);
            this.capacity = capacity;
        }
        return closeable;
    }

    @Override
    public void close() {
        if (closeable != null) {
            closeable.close();
            capacity = 0;
            closeable = null;
        }
    }
}
