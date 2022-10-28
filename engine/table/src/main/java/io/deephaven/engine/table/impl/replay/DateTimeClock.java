package io.deephaven.engine.table.impl.replay;

import io.deephaven.base.clock.Clock;
import io.deephaven.time.DateTime;

import java.time.Instant;

abstract class DateTimeClock implements Clock {

    public abstract DateTime currentDateTime();

    @Override
    public long currentTimeMillis() {
        return currentDateTime().getMillis();
    }

    @Override
    public long currentTimeMicros() {
        return currentDateTime().getMicros();
    }

    @Override
    public long currentTimeNanos() {
        return currentDateTime().getNanos();
    }

    @Override
    public Instant instantNanos() {
        return currentDateTime().getInstant();
    }

    @Override
    public Instant instantMillis() {
        return currentDateTime().getInstant();
    }
}
