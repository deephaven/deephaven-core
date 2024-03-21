//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.time.Duration;

/**
 * An {@link UpdateBySpec} for performing a windowed rolling sum operation
 */
@Immutable
@BuildableStyle
public abstract class RollingStdSpec extends RollingOpSpec {

    public static RollingStdSpec ofTicks(long revTicks) {
        return of(WindowScale.ofTicks(revTicks));
    }

    public static RollingStdSpec ofTicks(long revTicks, long fwdTicks) {
        return of(WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks));
    }

    public static RollingStdSpec ofTime(final String timestampCol, Duration revDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingStdSpec ofTime(final String timestampCol, Duration revDuration, Duration fwdDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingStdSpec ofTime(final String timestampCol, long revDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingStdSpec ofTime(final String timestampCol, long revDuration, long fwdDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingStdSpec of(WindowScale revWindowScale) {
        return ImmutableRollingStdSpec.builder().revWindowScale(revWindowScale).build();
    }

    public static RollingStdSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale) {
        return ImmutableRollingStdSpec.builder().revWindowScale(revWindowScale).fwdWindowScale(fwdWindowScale).build();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return
        // is primitive or boxed numeric
        applicableToNumeric(inputType)
                // is char?
                || inputType == char.class || inputType == Character.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
