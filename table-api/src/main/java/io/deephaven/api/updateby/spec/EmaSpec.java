package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.updateby.EmaControl;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.time.Duration;

/**
 * A {@link UpdateBySpec} for performing an Exponential Moving Average across the specified columns
 */
@Immutable
@BuildableStyle
public abstract class EmaSpec extends UpdateBySpecBase {
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
        return
        // is primitive numeric?
        inputType.equals(double.class) || inputType.equals(float.class)
                || inputType.equals(int.class) || inputType.equals(long.class) || inputType.equals(short.class)
                || inputType.equals(byte.class)

                // is boxed numeric?
                || Number.class.isAssignableFrom(inputType);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
