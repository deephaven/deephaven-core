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

    public static RollingSumSpec ofTicks(long revTicks) {
        return of(WindowScale.ofTicks(revTicks));
    }

    public static RollingSumSpec ofTicks(long revTicks, long fwdTicks) {
        return of(WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks));
    }

    public static RollingSumSpec ofTime(final String timestampCol, Duration revDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingSumSpec ofTime(final String timestampCol, Duration revDuration, Duration fwdDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingSumSpec ofTime(final String timestampCol, long revDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingSumSpec ofTime(final String timestampCol, long revDuration, long fwdDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    // internal use constructors

    private static RollingSumSpec of(WindowScale revWindowScale) {
        return ImmutableRollingSumSpec.builder().revTimeScale(revWindowScale).build();
    }

    private static RollingSumSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale) {
        // We would like to use jdk.internal.util.ArraysSupport.MAX_ARRAY_LENGTH, but it is not exported
        final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

        // assert some rational constraints
        final long size = revWindowScale.timescaleUnits() + fwdWindowScale.timescaleUnits();
        if (size < 0) {
            throw new IllegalArgumentException("UpdateBy rolling window size must be non-negative");
        } else if (!revWindowScale.isTimeBased() && size > MAX_ARRAY_SIZE) {
            throw new IllegalArgumentException(
                    "UpdateBy rolling window size may not exceed MAX_ARRAY_SIZE (" + MAX_ARRAY_SIZE + ")");
        }
        return ImmutableRollingSumSpec.builder().revTimeScale(revWindowScale).fwdTimeScale(fwdWindowScale).build();
    }

    public abstract Optional<OperationControl> control();

    public abstract WindowScale revTimeScale();

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
        // is primitive or boxed numeric
        applicableToNumeric(inputType)
                // is boolean?
                || inputType == boolean.class || inputType == Boolean.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
