package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * A {@link UpdateBySpec} for performing a windowed rolling weighted average across the specified columns
 */
@Immutable
@BuildableStyle
public abstract class RollingWAvgSpec extends RollingOpSpec {

    @Value.Parameter
    public abstract String weightCol();

    public static RollingWAvgSpec ofTicks(final String weightCol, long revTicks) {
        return of(weightCol, WindowScale.ofTicks(revTicks));
    }

    public static RollingWAvgSpec ofTicks(final String weightCol, long revTicks, long fwdTicks) {
        return of(weightCol, WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks));
    }

    public static RollingWAvgSpec ofTime(final String weightCol, final String timestampCol, Duration revDuration) {
        return of(weightCol, WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingWAvgSpec ofTime(final String weightCol, final String timestampCol, Duration revDuration, Duration fwdDuration) {
        return of(weightCol,
                WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    public static RollingWAvgSpec ofTime(final String weightCol, final String timestampCol, long revDuration) {
        return of(weightCol,WindowScale.ofTime(timestampCol, revDuration));
    }

    public static RollingWAvgSpec ofTime(final String weightCol, final String timestampCol, long revDuration, long fwdDuration) {
        return of(weightCol,
                WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration));
    }

    // internal use constructors
    private static RollingWAvgSpec of(String weightCol, WindowScale revWindowScale) {
        return ImmutableRollingWAvgSpec.builder()
                .weightCol(weightCol)
                .revWindowScale(revWindowScale)
                .build();
    }

    private static RollingWAvgSpec of(String weightCol, WindowScale revWindowScale, WindowScale fwdWindowScale) {
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
        applicableToNumeric(inputType);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
