//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import javax.annotation.Nullable;
import java.time.Duration;

@Immutable
@SimpleStyle
public abstract class WindowScale {
    public static WindowScale ofTime(final String timestampCol, long timeScaleNanos) {
        return ImmutableWindowScale.of(timestampCol, 0, timeScaleNanos);
    }

    public static WindowScale ofTime(final String timestampCol, Duration duration) {
        return ImmutableWindowScale.of(timestampCol, 0, duration.toNanos());
    }

    public static WindowScale ofTicks(double tickWindow) {
        return ImmutableWindowScale.of(null, tickWindow, 0);
    }

    @Parameter
    @Nullable
    public abstract String timestampCol();

    /**
     * Store the tick units as a double and convert to long as needed.
     */
    @Parameter
    public abstract double tickUnits();

    /**
     * Store the time units as a long (in nanoseconds).
     */
    @Parameter
    public abstract long timeUnits();


    public final boolean isTimeBased() {
        return timestampCol() != null;
    }

    /**
     * Return the appropriate tick/time units as a long
     */
    public final long getTimeScaleUnits() {
        return isTimeBased() ? timeUnits() : (long) tickUnits();
    }

    /**
     * Return the appropriate tick/time units as a double
     */
    public final double getFractionalTimeScaleUnits() {
        return isTimeBased() ? (double) timeUnits() : tickUnits();
    }

    public final Duration getDuration() {
        if (!isTimeBased()) {
            throw new IllegalStateException("getDuration() cannot be called on a tick-based Timescale");
        }
        return Duration.ofNanos(timeUnits());
    }

    public final long getTicks() {
        if (isTimeBased()) {
            throw new IllegalStateException("getTicks() cannot be called on a time-based Timescale");
        }
        return (long) tickUnits();
    }

    @Value.Check
    final void checkTimestampColEmpty() {
        if (timestampCol() != null && timestampCol().isEmpty()) {
            throw new IllegalArgumentException("WindowScale.timestampCol() must not be an empty string");
        }
    }
}
