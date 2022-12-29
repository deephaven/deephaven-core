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

    public static RollingSumSpec ofTicks(long tickWindow) {
        return of(WindowScale.ofTicks(tickWindow));
    }

    public static RollingSumSpec ofTicks(long prevTickWindow, long fwdTickWindow) {
        return of(WindowScale.ofTicks(prevTickWindow), WindowScale.ofTicks(fwdTickWindow));
    }

    public static RollingSumSpec ofTime(final String timestampCol, Duration prevWindowDuration) {
        return of(WindowScale.ofTime(timestampCol, prevWindowDuration));
    }

    public static RollingSumSpec ofTime(final String timestampCol, Duration prevWindowDuration,
            Duration fwdWindowDuration) {
        return of(WindowScale.ofTime(timestampCol, prevWindowDuration),
                WindowScale.ofTime(timestampCol, fwdWindowDuration));
    }

    // general use constructors

    public static RollingSumSpec of(WindowScale prevWindowScale) {
        return ImmutableRollingSumSpec.builder().prevTimeScale(prevWindowScale).build();
    }

    public static RollingSumSpec of(WindowScale prevWindowScale, WindowScale fwdWindowScale) {
        return ImmutableRollingSumSpec.builder().prevTimeScale(prevWindowScale).fwdTimeScale(fwdWindowScale).build();
    }

    // public static RollingSumSpec of(WindowScale prevTimeScale) {
    // return ImmutableWindowedOpSpec.builder().prevTimeScale(prevTimeScale).build();
    // }
    //
    // public static RollingSumSpec of(OperationControl control, WindowScale prevTimeScale, WindowScale fwdTimeScale) {
    // return
    // ImmutableWindowedOpSpec.builder().control(control).prevTimeScale(prevTimeScale).fwdTimeScale(fwdTimeScale).build();
    // }
    //
    // public static RollingSumSpec ofTime(final OperationControl control,
    // final String timestampCol,
    // long prevWindowTimeScaleNanos) {
    // return of(control, WindowScale.ofTime(timestampCol, prevWindowTimeScaleNanos));
    // }
    //
    // public static RollingSumSpec ofTime(final OperationControl control,
    // final String timestampCol,
    // long prevWindowTimeScaleNanos,
    // long fwdWindowTimeScaleNanos) {
    // return of(control, WindowScale.ofTime(timestampCol, prevWindowTimeScaleNanos), WindowScale.ofTime(timestampCol,
    // fwdWindowTimeScaleNanos));
    // }
    //
    // public static RollingSumSpec ofTime(final OperationControl control,
    // final String timestampCol,
    // Duration prevWindowDuration) {
    // return of(control, WindowScale.ofTime(timestampCol, prevWindowDuration));
    // }
    //
    //
    // public static RollingSumSpec ofTime(final OperationControl control,
    // final String timestampCol,
    // Duration prevWindowDuration,
    // Duration fwdWindowDuration) {
    // return of(control, WindowScale.ofTime(timestampCol, prevWindowDuration), WindowScale.ofTime(timestampCol,
    // fwdWindowDuration));
    // }
    //
    // public static RollingSumSpec ofTicks(OperationControl control, long prevTickWindow) {
    // return of(control, WindowScale.ofTicks(prevTickWindow));
    // }
    //
    // public static RollingSumSpec ofTicks(OperationControl control, long prevTickWindow, long fwdTickWindow) {
    // return of(control, WindowScale.ofTicks(prevTickWindow), WindowScale.ofTicks(fwdTickWindow));
    // }


    public abstract Optional<OperationControl> control();

    public abstract WindowScale prevTimeScale();

    // provide a default forward-looking timescale
    @Value.Default
    public WindowScale fwdTimeScale() {
        return WindowScale.ofTicks(0);
    }

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
                || Number.class.isAssignableFrom(inputType)

                // is boolean?
                || inputType == boolean.class || inputType == Boolean.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
