//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.updateby.OperationControl;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.Optional;

/**
 * A {@link UpdateBySpec} for performing an Exponential Moving Average across the specified columns
 */
@Immutable
@BuildableStyle
public abstract class EmMinMaxSpec extends UpdateBySpecBase {

    public static EmMinMaxSpec of(OperationControl control, boolean isMax, WindowScale windowScale) {
        return ImmutableEmMinMaxSpec.builder().control(control).isMax(isMax).windowScale(windowScale).build();
    }

    public static EmMinMaxSpec of(boolean isMax, WindowScale windowScale) {
        return ImmutableEmMinMaxSpec.builder().isMax(isMax).windowScale(windowScale).build();
    }

    public static EmMinMaxSpec ofTime(OperationControl control,
            boolean isMax,
            String timestampCol,
            long timeScaleNanos) {
        return of(control, isMax, WindowScale.ofTime(timestampCol, timeScaleNanos));
    }

    public static EmMinMaxSpec ofTime(boolean isMax, String timestampCol, long timeScaleNanos) {
        return of(isMax, WindowScale.ofTime(timestampCol, timeScaleNanos));
    }

    public static EmMinMaxSpec ofTime(final OperationControl control,
            boolean isMax,
            final String timestampCol,
            Duration emaDuration) {
        return of(control, isMax, WindowScale.ofTime(timestampCol, emaDuration));
    }

    public static EmMinMaxSpec ofTime(boolean isMax, String timestampCol, Duration emaDuration) {
        return of(isMax, WindowScale.ofTime(timestampCol, emaDuration));
    }

    public static EmMinMaxSpec ofTicks(OperationControl control, boolean isMax, double tickWindow) {
        return of(control, isMax, WindowScale.ofTicks(tickWindow));
    }

    public static EmMinMaxSpec ofTicks(boolean isMax, double tickWindow) {
        return of(isMax, WindowScale.ofTicks(tickWindow));
    }

    @Value.Parameter
    public abstract boolean isMax();

    public abstract Optional<OperationControl> control();

    @Value.Parameter
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
