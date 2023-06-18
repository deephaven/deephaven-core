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
public abstract class EmStdSpec extends UpdateBySpecBase {

    public static EmStdSpec of(OperationControl control, WindowScale windowScale) {
        return ImmutableEmStdSpec.builder().control(control).windowScale(windowScale).build();
    }

    public static EmStdSpec of(WindowScale windowScale) {
        return ImmutableEmStdSpec.builder().windowScale(windowScale).build();
    }

    public static EmStdSpec ofTime(final OperationControl control,
            final String timestampCol,
            long timeScaleNanos) {
        return of(control, WindowScale.ofTime(timestampCol, timeScaleNanos));
    }

    public static EmStdSpec ofTime(final String timestampCol, long timeScaleNanos) {
        return of(WindowScale.ofTime(timestampCol, timeScaleNanos));
    }

    public static EmStdSpec ofTime(final OperationControl control,
            final String timestampCol,
            Duration emaDuration) {
        return of(control, WindowScale.ofTime(timestampCol, emaDuration));
    }

    public static EmStdSpec ofTime(final String timestampCol, Duration emaDuration) {
        return of(WindowScale.ofTime(timestampCol, emaDuration));
    }

    public static EmStdSpec ofTicks(OperationControl control, double tickWindow) {
        return of(control, WindowScale.ofTicks(tickWindow));
    }

    public static EmStdSpec ofTicks(double tickWindow) {
        return of(WindowScale.ofTicks(tickWindow));
    }

    public abstract Optional<OperationControl> control();

    public abstract WindowScale windowScale();

    public final OperationControl controlOrDefault() {
        return control().orElseGet(OperationControl::defaultInstance);
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        // is primitive or boxed numeric?
        return applicableToNumeric(inputType)
                || inputType == char.class || inputType == Character.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
