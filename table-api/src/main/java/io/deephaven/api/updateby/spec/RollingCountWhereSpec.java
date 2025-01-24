//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.time.Duration;

/**
 * An {@link UpdateBySpec} for performing a windowed rolling formula operation.
 */
@Immutable
@BuildableStyle
public abstract class RollingCountWhereSpec extends RollingOpSpec {
    @Value.Parameter
    public abstract ColumnName column();

    @Value.Parameter
    public abstract Filter filter();

    public static RollingCountWhereSpec ofTicks(long revTicks, String resultColumn, String... filters) {
        return of(WindowScale.ofTicks(revTicks), resultColumn, filters);
    }

    public static RollingCountWhereSpec ofTicks(long revTicks, long fwdTicks, String resultColumn, String... filters) {
        return of(WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks), resultColumn, filters);
    }

    public static RollingCountWhereSpec ofTicks(long revTicks, String resultColumn, Filter filter) {
        return of(WindowScale.ofTicks(revTicks), resultColumn, filter);
    }

    public static RollingCountWhereSpec ofTicks(long revTicks, long fwdTicks, String resultColumn, Filter filter) {
        return of(WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks), resultColumn, filter);
    }

    public static RollingCountWhereSpec ofTime(final String timestampCol, Duration revDuration, String resultColumn,
            String... filters) {
        return of(WindowScale.ofTime(timestampCol, revDuration), resultColumn, filters);
    }

    public static RollingCountWhereSpec ofTime(final String timestampCol, Duration revDuration, Duration fwdDuration,
            String resultColumn, String... filters) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                resultColumn, filters);
    }

    public static RollingCountWhereSpec ofTime(final String timestampCol, Duration revDuration, String resultColumn,
            Filter filter) {
        return of(WindowScale.ofTime(timestampCol, revDuration), resultColumn, filter);
    }

    public static RollingCountWhereSpec ofTime(final String timestampCol, Duration revDuration, Duration fwdDuration,
            String resultColumn, Filter filter) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                resultColumn, filter);
    }

    public static RollingCountWhereSpec ofTime(final String timestampCol, long revDuration, String resultColumn,
            String... filters) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                resultColumn, filters);
    }

    public static RollingCountWhereSpec ofTime(final String timestampCol, long revDuration, long fwdDuration,
            String resultColumn, String... filters) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                resultColumn, filters);
    }

    public static RollingCountWhereSpec ofTime(final String timestampCol, long revDuration, String resultColumn,
            Filter filter) {
        return of(WindowScale.ofTime(timestampCol, revDuration), resultColumn, filter);
    }

    public static RollingCountWhereSpec ofTime(final String timestampCol, long revDuration, long fwdDuration,
            String resultColumn, Filter filter) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                resultColumn, filter);
    }

    // Base methods for creating the RollingFormulaSpec

    public static RollingCountWhereSpec of(WindowScale revWindowScale, String resultColumn, String... filters) {
        return of(revWindowScale, resultColumn, Filter.and(Filter.from(filters)));
    }

    public static RollingCountWhereSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale, String resultColumn,
            String... filters) {
        return of(revWindowScale, fwdWindowScale, resultColumn, Filter.and(Filter.from(filters)));
    }

    public static RollingCountWhereSpec of(WindowScale revWindowScale, String resultColumn, Filter filter) {
        return ImmutableRollingCountWhereSpec.builder()
                .revWindowScale(revWindowScale)
                .column(ColumnName.of(resultColumn))
                .filter(filter)
                .build();
    }

    public static RollingCountWhereSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale, String resultColumn,
            Filter filter) {
        return ImmutableRollingCountWhereSpec.builder()
                .revWindowScale(revWindowScale)
                .fwdWindowScale(fwdWindowScale)
                .column(ColumnName.of(resultColumn))
                .filter(filter)
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
