/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.clock;

import io.deephaven.clock.RealTimeClock;

public interface Clock {

    long currentTimeMillis();

    long currentTimeMicros();

    long currentTimeNanos();

    final class Null implements Clock {
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

    final class SystemClock implements Clock {

        RealTimeClock REAL_TIME_CLOCK = RealTimeClock.loadImpl();

        @Override
        public long currentTimeMillis() {
            return REAL_TIME_CLOCK.currentTimeMillis();
        }

        @Override
        public long currentTimeMicros() {
            return REAL_TIME_CLOCK.currentTimeMicros();
        }

        @Override
        public long currentTimeNanos() {
            return REAL_TIME_CLOCK.currentTimeNanos();
        }
    }

    SystemClock SYSTEM = new SystemClock();

}
