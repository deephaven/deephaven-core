/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.clock;

public interface Clock {

    long currentTimeMillis();

    long currentTimeMicros();

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
    }

    Null NULL = new Null();

    /**
     * This implementation just returns the last value passed to set(). It allows for precise control over when clock
     * calls are made to the underlying system (e.g. AppClock.currentTimeMicros()).
     */
    class Cached implements Clock {
        private long cachedNowMicros;

        public void set(long nowMicros) {
            cachedNowMicros = nowMicros;
        }

        @Override
        public final long currentTimeMillis() {
            return cachedNowMicros / 1000;
        }

        @Override
        public final long currentTimeMicros() {
            return cachedNowMicros;
        }
    }

    /**
     * This implementation is similar to cached, except that is calls set() itself on a the Clock instance given to the
     * constructor exactly once between reset() calls.
     */
    class CachedOnDemand implements Clock {
        private final Clock realClock;
        private long cachedNowMicros;

        public CachedOnDemand(Clock realClock) {
            this.realClock = realClock;
        }

        public void set() {
            cachedNowMicros = realClock.currentTimeMicros();
        }

        public void reset() {
            cachedNowMicros = 0;
        }

        private long maybeUpdate() {
            if (cachedNowMicros == 0) {
                set();
            }
            return cachedNowMicros;
        }

        @Override
        public final long currentTimeMillis() {
            return maybeUpdate() / 1000;
        }

        @Override
        public final long currentTimeMicros() {
            return maybeUpdate();
        }
    }
}
