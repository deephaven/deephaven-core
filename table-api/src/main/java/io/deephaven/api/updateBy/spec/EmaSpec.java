package io.deephaven.api.updateBy.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.updateBy.EmaControl;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.time.Duration;

/**
 * A {@link UpdateBySpec} for performing an Exponential Moving Average across the specified columns
 */
@Immutable
@BuildableStyle
public abstract class EmaSpec implements UpdateBySpec {
    public static EmaSpec ofTime(final EmaControl control,
            final String timestampCol,
            long timeScaleNanos) {
        return ImmutableEmaSpec.builder()
                .control(control)
                .timeScale(TimeScale.ofTime(timestampCol, timeScaleNanos))
                .build();
    }

    public static EmaSpec ofTime(final String timestampCol, long timeScaleNanos) {
        return ImmutableEmaSpec.builder()
                .control(EmaControl.DEFAULT)
                .timeScale(TimeScale.ofTime(timestampCol, timeScaleNanos))
                .build();
    }

    public static EmaSpec ofTime(final EmaControl control,
                                 final String timestampCol,
                                 Duration emaDuration) {
        return ImmutableEmaSpec.builder()
                .control(control)
                .timeScale(TimeScale.ofTime(timestampCol, emaDuration))
                .build();
    }

    public static EmaSpec ofTime(final String timestampCol, Duration emaDuration) {
        return ImmutableEmaSpec.builder()
                .control(EmaControl.DEFAULT)
                .timeScale(TimeScale.ofTime(timestampCol, emaDuration))
                .build();
    }

    public static EmaSpec ofTicks(EmaControl control, long tickWindow) {
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
    public final boolean applicableTo(Class<?> inputType) {
        boolean isPrimitiveNumeric = inputType.equals(double.class) || inputType.equals(float.class)
                || inputType.equals(int.class) || inputType.equals(long.class) || inputType.equals(short.class)
                || inputType.equals(byte.class);
        boolean isBoxedNumeric = Number.class.isAssignableFrom(inputType);

        return isPrimitiveNumeric || isBoxedNumeric;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
