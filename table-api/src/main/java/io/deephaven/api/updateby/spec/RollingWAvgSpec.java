//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.time.Duration;

/**
 * A {@link UpdateBySpec} for performing a windowed rolling weighted average across the specified columns
 */
@Immutable
@BuildableStyle
public abstract class RollingWAvgSpec extends RollingOpSpec {

    public abstract String weightCol();

    public static RollingWAvgSpec ofTicks(long revTicks, final String weightCol) {
        return of(WindowScale.ofTicks(revTicks), weightCol);
    }

    public static RollingWAvgSpec ofTicks(long revTicks, long fwdTicks, final String weightCol) {
        return of(WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks), weightCol);
    }

    public static RollingWAvgSpec ofTime(final String timestampCol, Duration revDuration, final String weightCol) {
        return of(WindowScale.ofTime(timestampCol, revDuration), weightCol);
    }

    public static RollingWAvgSpec ofTime(final String timestampCol, Duration revDuration, Duration fwdDuration,
            final String weightCol) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                weightCol);
    }

    public static RollingWAvgSpec ofTime(final String timestampCol, long revDuration, final String weightCol) {
        return of(WindowScale.ofTime(timestampCol, revDuration), weightCol);
    }

    public static RollingWAvgSpec ofTime(final String timestampCol, long revDuration, long fwdDuration,
            final String weightCol) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                weightCol);
    }

    public static RollingWAvgSpec of(WindowScale revWindowScale, final String weightCol) {
        return ImmutableRollingWAvgSpec.builder()
                .weightCol(weightCol)
                .revWindowScale(revWindowScale)
                .build();
    }

    public static RollingWAvgSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale, final String weightCol) {
        return ImmutableRollingWAvgSpec.builder()
                .weightCol(weightCol)
                .revWindowScale(revWindowScale)
                .fwdWindowScale(fwdWindowScale)
                .build();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return
        // is primitive or boxed numeric
        applicableToNumeric(inputType)
                // or char
                || inputType == char.class || inputType == Character.class;
    }

    public final boolean weightColumnApplicableTo(Class<?> inputType) {
        return
        // is primitive or boxed numeric
        applicableToNumeric(inputType)
                // or char
                || inputType == char.class || inputType == Character.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
