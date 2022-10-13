/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.clock;

public interface Clock {

    long currentTimeMillis();

    long currentTimeMicros();

    long currentTimeNanos();

    interface Factory {
        Clock getClock();
    }

    class Null implements Clock {
        @Override
        public long currentTimeMillis() {
            return 0;
        }

        @Override
        public long currentTimeMicros() {
            return 0;
        }

        @Override
        public long currentTimeNanos() {
            return 0;
        }
    }

    Null NULL = new Null();
}
