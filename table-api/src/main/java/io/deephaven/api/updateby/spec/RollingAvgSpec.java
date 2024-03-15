//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.time.Duration;

/**
 * A {@link UpdateBySpec} for performing a windowed rolling average across the specified columns
 */
@Immutable
@BuildableStyle
public abstract class RollingAvgSpec extends RollingOpSpec {

    public static RollingAvgSpec ofTicks(long revTicks) {
        return of(WindowScale.ofTicks(revTicks));
    }

    public static RollingAvgSpec ofTicks(long revTicks, long fwdTicks) {
        return of(WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks));
    }

    public static RollingAvgSpec ofTime(final String timestampCol, Duration revDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingAvgSpec ofTime(final String timestampCol, Duration revDuration, Duration fwdDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingAvgSpec ofTime(final String timestampCol, long revDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingAvgSpec ofTime(final String timestampCol, long revDuration, long fwdDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingAvgSpec of(WindowScale revWindowScale) {
        return ImmutableRollingAvgSpec.builder().revWindowScale(revWindowScale).build();
    }

    public static RollingAvgSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale) {
        return ImmutableRollingAvgSpec.builder().revWindowScale(revWindowScale).fwdWindowScale(fwdWindowScale).build();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return
        // is primitive or boxed numeric
        applicableToNumeric(inputType)
                || inputType == char.class || inputType == Character.class
                // is boolean?
                || inputType == boolean.class || inputType == Boolean.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
