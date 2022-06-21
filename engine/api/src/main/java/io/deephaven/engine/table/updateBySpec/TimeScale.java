package io.deephaven.engine.table.updateBySpec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;
import org.jetbrains.annotations.Nullable;

@Immutable
@SimpleStyle
public abstract class TimeScale {
    @Parameter
    @Nullable
    public abstract String timestampCol();

    @Parameter
    public abstract long timescaleUnits();

    public static TimeScale ofTime(final String timestampCol, long timeScaleNanos) {
        return ImmutableTimeScale.of(timestampCol, timeScaleNanos);
    }

    public static TimeScale ofTicks(long tickWindow) {
        return ImmutableTimeScale.of(null, tickWindow);
    }

    public boolean isTimeBased() {
        return timestampCol() != null;
    }

    @Value.Check
    final void checkTimestampColEmpty() {
        if (timestampCol() != null && timestampCol().isEmpty()) {
            throw new IllegalArgumentException("TimeScale.timestampCol() must not be an empty string");
        }
    }

}
