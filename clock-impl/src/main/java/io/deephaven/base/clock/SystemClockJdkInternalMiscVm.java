//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.clock;

import com.google.auto.service.AutoService;
import jdk.internal.misc.VM;

import java.time.Clock;
import java.time.Instant;

/**
 * A clock based off of the current system time, as exposed via {@link VM#getNanoTimeAdjustment(long)}.
 *
 * <p>
 * Equivalent semantics to {@link SystemClockUtc}, but without the {@link Instant} allocation.
 */
@AutoService(SystemClock.class)
public final class SystemClockJdkInternalMiscVm implements SystemClock {

    public SystemClockJdkInternalMiscVm() {
        // Ensure we fail ASAP if jdk.internal.misc is not exposed, or adj == -1
        currentTimeNanos();
    }

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public long currentTimeMicros() {
        return currentTimeNanos() / 1_000;
    }

    @Override
    public long currentTimeNanos() {
        final long adj = VM.getNanoTimeAdjustment(0);
        if (adj == -1) {
            throw new IllegalStateException(
                    "VM.getNanoTimeAdjustment bounds have been broken - current time is too large to work with this implementation.");
        }
        return adj;
    }

    @Override
    public Instant instantNanos() {
        return Clock.systemUTC().instant();
    }

    @Override
    public Instant instantMillis() {
        // See note in java.time.Clock.SystemClock#millis, this is the same source as java.time.Clock.systemUTC()
        return Instant.ofEpochMilli(System.currentTimeMillis());
    }
}
