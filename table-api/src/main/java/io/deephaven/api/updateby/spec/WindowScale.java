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
        return ImmutableWindowScale.of(timestampCol, timeScaleNanos);
    }

    public static WindowScale ofTime(final String timestampCol, double timeScaleNanos) {
        return ImmutableWindowScale.of(timestampCol, timeScaleNanos);
    }

    public static WindowScale ofTime(final String timestampCol, Duration duration) {
        return ImmutableWindowScale.of(timestampCol, duration.toNanos());
    }

    public static WindowScale ofTicks(long tickWindow) {
        return ImmutableWindowScale.of(null, tickWindow);
    }

    public static WindowScale ofTicks(double tickWindow) {
        return ImmutableWindowScale.of(null, tickWindow);
    }

    @Parameter
    @Nullable
    public abstract String timestampCol();

    /**
     * Store the timescale units as a double and convert to long as needed.
     */
    @Parameter
    public abstract double timescaleUnits();

    public final boolean isTimeBased() {
        return timestampCol() != null;
    }

    public final long longTimescaleUnits() {
        return (long) timescaleUnits();
    }

    public final Duration getDuration() {
        if (!isTimeBased()) {
            throw new IllegalStateException("getDuration() cannot be called on a tick-based Timescale");
        }
        return Duration.ofNanos((long) timescaleUnits());
    }

    public final long getTicks() {
        if (isTimeBased()) {
            throw new IllegalStateException("getTicks() cannot be called on a time-based Timescale");
        }
        return (long) timescaleUnits();
    }

    public final double getFractionalTicks() {
        if (isTimeBased()) {
            throw new IllegalStateException("getFractionalTicks() cannot be called on a time-based Timescale");
        }
        return timescaleUnits();
    }

    @Value.Check
    final void checkTimestampColEmpty() {
        if (timestampCol() != null && timestampCol().isEmpty()) {
            throw new IllegalArgumentException("WindowScale.timestampCol() must not be an empty string");
        }
    }
}
