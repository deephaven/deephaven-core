package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.updateby.OperationControl;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.Optional;

/**
 * A {@link UpdateBySpec} for performing a windowed rolling sum across the specified columns
 */
@Immutable
@BuildableStyle
public abstract class RollingSumSpec extends UpdateBySpecBase {

    // most common usages first, will complete the list later

    public static RollingSumSpec ofTicks(long prevTicks) {
        return of(WindowScale.ofTicks(prevTicks));
    }

    public static RollingSumSpec ofTicks(long prevTicks, long fwdTicks) {
        return of(WindowScale.ofTicks(prevTicks), WindowScale.ofTicks(fwdTicks));
    }

    public static RollingSumSpec ofTime(final String timestampCol, Duration prevDuration) {
        return of(WindowScale.ofTime(timestampCol, prevDuration));
    }

    public static RollingSumSpec ofTime(final String timestampCol, Duration prevDuration, Duration fwdDuration) {
        return of(WindowScale.ofTime(timestampCol, prevDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingSumSpec ofTime(final String timestampCol, long prevDuration) {
        return of(WindowScale.ofTime(timestampCol, prevDuration));
    }

    public static RollingSumSpec ofTime(final String timestampCol, long prevDuration, long fwdDuration) {
        return of(WindowScale.ofTime(timestampCol, prevDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    // general use constructors

    public static RollingSumSpec of(WindowScale prevWindowScale) {
        return ImmutableRollingSumSpec.builder().prevTimeScale(prevWindowScale).build();
    }

    public static RollingSumSpec of(WindowScale prevWindowScale, WindowScale fwdWindowScale) {
        return ImmutableRollingSumSpec.builder().prevTimeScale(prevWindowScale).fwdTimeScale(fwdWindowScale).build();
    }

    public abstract Optional<OperationControl> control();

    public abstract WindowScale prevTimeScale();

    /**
     * provide a default forward-looking timescale
     */
    @Value.Default
    public WindowScale fwdTimeScale() {
        return WindowScale.ofTicks(0);
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return
        // is primitive numeric?
        inputType == double.class || inputType == float.class
                || inputType == int.class || inputType == long.class || inputType == short.class
                || inputType == byte.class

                // is boxed numeric?
                || Number.class.isAssignableFrom(inputType)

                // is boolean?
                || inputType == boolean.class || inputType == Boolean.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
