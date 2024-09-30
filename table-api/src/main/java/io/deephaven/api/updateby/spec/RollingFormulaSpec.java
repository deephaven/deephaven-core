//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * An {@link UpdateBySpec} for performing a windowed rolling formula operation.
 */
@Immutable
@BuildableStyle
public abstract class RollingFormulaSpec extends RollingOpSpec {

    public abstract List<String> formula();

    // Continue single-parameter token support
    public abstract Optional<String> paramToken();

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

    // New methods for supporting the non-tokenized version (expects valid column names in the formula)

    public static RollingFormulaSpec ofTicks(long revTicks, String... formula) {
        return of(WindowScale.ofTicks(revTicks), formula);
    }

    public static RollingFormulaSpec ofTicks(long revTicks, long fwdTicks, String... formula) {
        return of(WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks), formula);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, Duration revDuration, String... formula) {
        return of(WindowScale.ofTime(timestampCol, revDuration), formula);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, Duration revDuration, Duration fwdDuration,
            String... formula) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                formula);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, long revDuration, String... formula) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                formula);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, long revDuration, long fwdDuration,
            String... formula) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                formula);
    }

    // Base methods for creating the RollingFormulaSpec

    public static RollingFormulaSpec of(WindowScale revWindowScale, String formula, String paramToken) {
        return ImmutableRollingFormulaSpec.builder()
                .revWindowScale(revWindowScale)
                .addFormula(formula)
                .paramToken(paramToken)
                .build();
    }

    public static RollingFormulaSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale, String formula,
            String paramToken) {
        return ImmutableRollingFormulaSpec.builder()
                .revWindowScale(revWindowScale)
                .fwdWindowScale(fwdWindowScale)
                .addFormula(formula)
                .paramToken(paramToken)
                .build();
    }

    public static RollingFormulaSpec of(WindowScale revWindowScale, String[] formula) {
        return ImmutableRollingFormulaSpec.builder()
                .revWindowScale(revWindowScale)
                .addFormula(formula)
                .build();
    }

    public static RollingFormulaSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale, String[] formula) {
        return ImmutableRollingFormulaSpec.builder()
                .revWindowScale(revWindowScale)
                .fwdWindowScale(fwdWindowScale)
                .addFormula(formula)
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
    final void checkFormulaTokenPresent() {
        if (paramToken().isPresent()) {
            if (formula().size() != 1) {
                throw new IllegalArgumentException("paramToken requires a single formula");
            }
        }
    }
}
