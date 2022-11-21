package io.deephaven.util.locks;

import io.deephaven.util.SafeCloseable;

import java.util.Objects;
import java.util.concurrent.locks.Lock;

final class SafeCloseableUnlock implements SafeCloseable {
    private final Lock lock;

    public SafeCloseableUnlock(Lock lock) {
        this.lock = Objects.requireNonNull(lock);
    }

    @Override
    public void close() {
        lock.unlock();
    }
}
