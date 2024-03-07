//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.time.Duration;

/**
 * An {@link UpdateBySpec} for performing a windowed rolling count operation
 */
@Immutable
@BuildableStyle
public abstract class RollingCountSpec extends RollingOpSpec {

    public static RollingCountSpec ofTicks(long revTicks) {
        return of(WindowScale.ofTicks(revTicks));
    }

    public static RollingCountSpec ofTicks(long revTicks, long fwdTicks) {
        return of(WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks));
    }

    public static RollingCountSpec ofTime(final String timestampCol, Duration revDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingCountSpec ofTime(final String timestampCol, Duration revDuration, Duration fwdDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingCountSpec ofTime(final String timestampCol, long revDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingCountSpec ofTime(final String timestampCol, long revDuration, long fwdDuration) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingCountSpec of(WindowScale revWindowScale) {
        return ImmutableRollingCountSpec.builder().revWindowScale(revWindowScale).build();
    }

    public static RollingCountSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale) {
        return ImmutableRollingCountSpec.builder().revWindowScale(revWindowScale).fwdWindowScale(fwdWindowScale)
                .build();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return true;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
