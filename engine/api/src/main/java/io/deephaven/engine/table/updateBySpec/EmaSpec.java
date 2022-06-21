package io.deephaven.engine.table.updateBySpec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.EmaControl;
import io.deephaven.util.type.TypeUtils;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Parameter;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link UpdateBySpec} for performing an Exponential Moving Average across the specified columns
 */
@Immutable
@BuildableStyle
public abstract class EmaSpec implements UpdateBySpec {
    // TimeScale is used internally to manage timed vs. tick EMA information
    public static class TimeScale {
        private final String timestampCol;
        private final long timescaleUnits;

        private TimeScale(String timestampCol, long timescaleUnits) {
            this.timestampCol = timestampCol;
            this.timescaleUnits = timescaleUnits;
        }

        public static TimeScale ofTime(@NotNull final String timestampCol, long timeScaleNanos) {
            return new TimeScale(timestampCol, timeScaleNanos);
        }

        public static TimeScale ofTicks(long tickWindow) {
            return new TimeScale(null, tickWindow);
        }

        public boolean isTimeBased() {
            return timestampCol != null;
        }

        public String timestampCol() {
            return timestampCol;
        }

        /**
         * This returns the timescale units (ticks or nanos)
         *
         * @return the timescale units (ticks or nanos)
         */
        public long timescaleUnits() {
            return timescaleUnits;
        }
    }

    public static EmaSpec ofTime(@NotNull final EmaControl control,
                                 @NotNull final String timestampCol,
                                 long timeScaleNanos) {
        return ImmutableEmaSpec.builder()
                .control(control)
                .timeScale(TimeScale.ofTime(timestampCol, timeScaleNanos))
                .build();
    }

    public static EmaSpec ofTime(@NotNull final String timestampCol, long timeScaleNanos) {
        return ImmutableEmaSpec.builder()
                .control(EmaControl.DEFAULT)
                .timeScale(TimeScale.ofTime(timestampCol, timeScaleNanos))
                .build();
    }

    public static EmaSpec ofTicks(@NotNull EmaControl control, long tickWindow) {
        return ImmutableEmaSpec.builder()
                .control(control)
                .timeScale(TimeScale.ofTicks(tickWindow))
                .build();
    }

    public static EmaSpec ofTicks(long tickWindow) {
        return ImmutableEmaSpec.builder()
                .control(EmaControl.DEFAULT)
                .timeScale(TimeScale.ofTicks(tickWindow))
                .build();
    }

    @Parameter
    public abstract EmaControl control();

    @Parameter
    public abstract TimeScale timeScale();

    @Override
    public final boolean applicableTo(@NotNull Class<?> inputType) {
        return TypeUtils.isNumeric(inputType);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkTimeStampColSet() {
        if (timeScale().timestampCol() != null && timeScale().timestampCol().isEmpty()) {
            throw new IllegalArgumentException("timeScale().timestampCol() must not be an empty string");
        }
    }
}
