//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.time.Duration;

/**
 * An {@link UpdateBySpec} for performing a windowed rolling formula operation.
 */
@Immutable
@BuildableStyle
public abstract class RollingFormulaSpec extends RollingOpSpec {

    public abstract String formula();

    public abstract String paramToken();

    public static RollingFormulaSpec ofTicks(long revTicks, String formula, String paramToken) {
        return of(WindowScale.ofTicks(revTicks), formula, paramToken);
    }

    public static RollingFormulaSpec ofTicks(long revTicks, long fwdTicks, String formula, String paramToken) {
        return of(WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks), formula, paramToken);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, Duration revDuration, String formula,
            String paramToken) {
        return of(WindowScale.ofTime(timestampCol, revDuration), formula, paramToken);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, Duration revDuration, Duration fwdDuration,
            String formula, String paramToken) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                formula, paramToken);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, long revDuration, String formula,
            String paramToken) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                formula, paramToken);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, long revDuration, long fwdDuration,
            String formula, String paramToken) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                formula, paramToken);
    }

    public static RollingFormulaSpec of(WindowScale revWindowScale, String formula, String paramToken) {
        return ImmutableRollingFormulaSpec.builder()
                .revWindowScale(revWindowScale)
                .formula(formula)
                .paramToken(paramToken)
                .build();
    }

    public static RollingFormulaSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale, String formula,
            String paramToken) {
        return ImmutableRollingFormulaSpec.builder()
                .revWindowScale(revWindowScale)
                .fwdWindowScale(fwdWindowScale)
                .formula(formula)
                .paramToken(paramToken)
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

    @Value.Check
    final void checkFormula() {
        if (formula().isEmpty()) {
            throw new IllegalArgumentException("formula must not be empty");
        }
    }

    @Value.Check
    final void checkParamToken() {
        if (paramToken().isEmpty()) {
            throw new IllegalArgumentException("paramToken must not be empty");
        }
    }
}
