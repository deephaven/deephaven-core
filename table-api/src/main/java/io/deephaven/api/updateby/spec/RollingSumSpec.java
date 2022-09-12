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
        return of(TimeScale.ofTicks(tickWindow));
    }

    public static RollingSumSpec ofTicks(long prevTickWindow, long fwdTickWindow) {
        return of(TimeScale.ofTicks(prevTickWindow), TimeScale.ofTicks(fwdTickWindow));
    }

    public static RollingSumSpec ofTime(final String timestampCol, Duration prevWindowDuration) {
        return of(TimeScale.ofTime(timestampCol, prevWindowDuration));
    }

    public static RollingSumSpec ofTime(final String timestampCol, Duration prevWindowDuration, Duration fwdWindowDuration) {
        return of(TimeScale.ofTime(timestampCol, prevWindowDuration), TimeScale.ofTime(timestampCol, fwdWindowDuration));
    }

    // general use contructors

    public static RollingSumSpec of(TimeScale prevTimeScale) {
        return ImmutableRollingSumSpec.builder().prevTimeScale(prevTimeScale).build();
    }

    public static RollingSumSpec of(TimeScale prevTimeScale, TimeScale fwdTimeScale) {
        return ImmutableRollingSumSpec.builder().prevTimeScale(prevTimeScale).fwdTimeScale(fwdTimeScale).build();
    }

//    public static RollingSumSpec of(TimeScale prevTimeScale) {
//        return ImmutableWindowedOpSpec.builder().prevTimeScale(prevTimeScale).build();
//    }
//
//    public static RollingSumSpec of(OperationControl control, TimeScale prevTimeScale, TimeScale fwdTimeScale) {
//        return ImmutableWindowedOpSpec.builder().control(control).prevTimeScale(prevTimeScale).fwdTimeScale(fwdTimeScale).build();
//    }
//
//    public static RollingSumSpec ofTime(final OperationControl control,
//                                            final String timestampCol,
//                                            long prevWindowTimeScaleNanos) {
//        return of(control, TimeScale.ofTime(timestampCol, prevWindowTimeScaleNanos));
//    }
//
//    public static RollingSumSpec ofTime(final OperationControl control,
//                                            final String timestampCol,
//                                            long prevWindowTimeScaleNanos,
//                                            long fwdWindowTimeScaleNanos) {
//        return of(control, TimeScale.ofTime(timestampCol, prevWindowTimeScaleNanos), TimeScale.ofTime(timestampCol, fwdWindowTimeScaleNanos));
//    }
//
//    public static RollingSumSpec ofTime(final OperationControl control,
//                                            final String timestampCol,
//                                            Duration prevWindowDuration) {
//        return of(control, TimeScale.ofTime(timestampCol, prevWindowDuration));
//    }
//
//
//    public static RollingSumSpec ofTime(final OperationControl control,
//                                            final String timestampCol,
//                                            Duration prevWindowDuration,
//                                            Duration fwdWindowDuration) {
//        return of(control, TimeScale.ofTime(timestampCol, prevWindowDuration), TimeScale.ofTime(timestampCol, fwdWindowDuration));
//    }
//
//    public static RollingSumSpec ofTicks(OperationControl control, long prevTickWindow) {
//        return of(control, TimeScale.ofTicks(prevTickWindow));
//    }
//
//    public static RollingSumSpec ofTicks(OperationControl control, long prevTickWindow, long fwdTickWindow) {
//        return of(control, TimeScale.ofTicks(prevTickWindow), TimeScale.ofTicks(fwdTickWindow));
//    }


    public abstract Optional<OperationControl> control();

    public abstract TimeScale prevTimeScale();

    // provide a default forward-looking timescale
    @Value.Default
    public TimeScale fwdTimeScale() {
        return TimeScale.ofTicks(0);
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
