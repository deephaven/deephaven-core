package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.updateby.OperationControl;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.Optional;

/**
 * A {@link UpdateBySpec} for performing an Exponential Moving Average across the specified columns
 */
@Immutable
@BuildableStyle
public abstract class EmaSpec extends UpdateBySpecBase {

    public static EmaSpec of(OperationControl control, TimeScale timeScale) {
        return ImmutableEmaSpec.builder().control(control).timeScale(timeScale).build();
    }

    public static EmaSpec of(TimeScale timeScale) {
        return ImmutableEmaSpec.builder().timeScale(timeScale).build();
    }

    public static EmaSpec ofTime(final OperationControl control,
            final String timestampCol,
            long timeScaleNanos) {
        return of(control, TimeScale.ofTime(timestampCol, timeScaleNanos));
    }

    public static EmaSpec ofTime(final String timestampCol, long timeScaleNanos) {
        return of(TimeScale.ofTime(timestampCol, timeScaleNanos));
    }

    public static EmaSpec ofTime(final OperationControl control,
            final String timestampCol,
            Duration emaDuration) {
        return of(control, TimeScale.ofTime(timestampCol, emaDuration));
    }

    public static EmaSpec ofTime(final String timestampCol, Duration emaDuration) {
        return of(TimeScale.ofTime(timestampCol, emaDuration));
    }

    public static EmaSpec ofTicks(OperationControl control, long tickWindow) {
        return of(control, TimeScale.ofTicks(tickWindow));
    }

    public static EmaSpec ofTicks(long tickWindow) {
        return of(TimeScale.ofTicks(tickWindow));
    }

    public abstract Optional<OperationControl> control();

    public abstract TimeScale timeScale();

    public final OperationControl controlOrDefault() {
        return control().orElseGet(OperationControl::defaultInstance);
    }

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
