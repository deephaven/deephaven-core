package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import javax.annotation.Nullable;
import java.time.Duration;

@Immutable
@SimpleStyle
public abstract class TimeScale {
    public static TimeScale ofTime(final String timestampCol, long timeScaleNanos) {
        return ImmutableTimeScale.of(timestampCol, timeScaleNanos);
    }

    public static TimeScale ofTime(final String timestampCol, Duration duration) {
        return ImmutableTimeScale.of(timestampCol, duration.toNanos());
    }

    public static TimeScale ofTicks(long tickWindow) {
        return ImmutableTimeScale.of(null, tickWindow);
    }

    @Parameter
    @Nullable
    public abstract String timestampCol();

    @Parameter
    public abstract long timescaleUnits();

    public final boolean isTimeBased() {
        return timestampCol() != null;
    }

    public final Duration getDuration() {
        if (!isTimeBased()) {
            throw new IllegalStateException("getDuration() cannot be called on a tick-based Timescale");
        }
        return Duration.ofNanos(timescaleUnits());
    }

    public final long getTicks() {
        if (isTimeBased()) {
            throw new IllegalStateException("getTicks() cannot be called on a time-based Timescale");
        }
        return timescaleUnits();
    }

    @Value.Check
    final void checkTimestampColEmpty() {
        if (timestampCol() != null && timestampCol().isEmpty()) {
            throw new IllegalArgumentException("TimeScale.timestampCol() must not be an empty string");
        }
    }
}
