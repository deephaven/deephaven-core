//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.time.Duration;

/**
 * A {@link UpdateBySpec} for performing a windowed rolling sum across the specified columns
 */
@Immutable
@BuildableStyle
public abstract class RollingMinMaxSpec extends RollingOpSpec {

    public static RollingMinMaxSpec ofTicks(boolean isMax, long revTicks) {
        return of(isMax, WindowScale.ofTicks(revTicks));
    }

    public static RollingMinMaxSpec ofTicks(boolean isMax, long revTicks, long fwdTicks) {
        return of(isMax, WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks));
    }

    public static RollingMinMaxSpec ofTime(boolean isMax, final String timestampCol, Duration revDuration) {
        return of(isMax, WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingMinMaxSpec ofTime(boolean isMax, final String timestampCol, Duration revDuration,
            Duration fwdDuration) {
        return of(isMax, WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingMinMaxSpec ofTime(boolean isMax, final String timestampCol, long revDuration) {
        return of(isMax, WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingMinMaxSpec ofTime(boolean isMax, final String timestampCol, long revDuration,
            long fwdDuration) {
        return of(isMax, WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingMinMaxSpec of(boolean isMax, WindowScale revWindowScale) {
        return ImmutableRollingMinMaxSpec.builder().isMax(isMax).revWindowScale(revWindowScale).build();
    }

    public static RollingMinMaxSpec of(boolean isMax, WindowScale revWindowScale, WindowScale fwdWindowScale) {
        return ImmutableRollingMinMaxSpec.builder().isMax(isMax)
                .revWindowScale(revWindowScale).fwdWindowScale(fwdWindowScale).build();
    }

    public abstract boolean isMax();

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return
        // is primitive or boxed numeric?
        applicableToNumeric(inputType)
                || inputType == char.class || inputType == Character.class
                // is comparable?
                || (Comparable.class.isAssignableFrom(inputType) && inputType != Boolean.class);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
