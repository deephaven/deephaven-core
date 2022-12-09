/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.clock;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;

enum SystemClockInstance implements SystemClock {
    INSTANCE;

    private final SystemClock delegate;

    SystemClockInstance() {
        try {
            delegate = SystemClock.of();
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

    @Override
    public long currentTimeMicros() {
        return delegate.currentTimeMicros();
    }

    @Override
    public long currentTimeNanos() {
        return delegate.currentTimeNanos();
    }

    @Override
    public Instant instantNanos() {
        return delegate.instantNanos();
    }

    @Override
    public Instant instantMillis() {
        return delegate.instantMillis();
    }
}
