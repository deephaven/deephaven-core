//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.Selectable;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.Optional;

/**
 * An {@link UpdateBySpec} for performing a windowed rolling formula operation.
 */
@Immutable
@BuildableStyle
public abstract class RollingFormulaSpec extends RollingOpSpec {
    /**
     * The formula to use to calculate output values. The formula is similar to
     * {@link io.deephaven.api.TableOperations#update} and {@link io.deephaven.api.TableOperations#updateView} in
     * specifying the output column name and the expression to compute in terms of the input columns. (e.g.
     * {@code "outputCol = sum(inputColA + inputColB)"}).
     * <p>
     * The alternative (and deprecated) form for {@link #formula()} is active when {@link #paramToken()} is provided. In
     * this case the formula should only contain the expression in terms of the token. (e.g. {@code sum(x)} where x is
     * the {@link #paramToken()}). NOTE: This form is deprecated and will be removed in a future release.
     */
    public abstract String formula();

    /**
     * (Deprecated) The token to use in {@link #formula()} to represent the input column. If this parameter is provided,
     * then only a single input column can be provided in the formula.
     */
    @Deprecated
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

    public static RollingFormulaSpec ofTicks(long revTicks, String formula) {
        return of(WindowScale.ofTicks(revTicks), formula);
    }

    public static RollingFormulaSpec ofTicks(long revTicks, long fwdTicks, String formula) {
        return of(WindowScale.ofTicks(revTicks), WindowScale.ofTicks(fwdTicks), formula);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, Duration revDuration, String formula) {
        return of(WindowScale.ofTime(timestampCol, revDuration), formula);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, Duration revDuration, Duration fwdDuration,
            String formula) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                formula);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, long revDuration, String formula) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                formula);
    }

    public static RollingFormulaSpec ofTime(final String timestampCol, long revDuration, long fwdDuration,
            String formula) {
        return of(WindowScale.ofTime(timestampCol, revDuration),
                WindowScale.ofTime(timestampCol, fwdDuration),
                formula);
    }

    // Base methods for creating the RollingFormulaSpec

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

    public static RollingFormulaSpec of(WindowScale revWindowScale, String formula) {
        return ImmutableRollingFormulaSpec.builder()
                .revWindowScale(revWindowScale)
                .formula(formula)
                .build();
    }

    public static RollingFormulaSpec of(WindowScale revWindowScale, WindowScale fwdWindowScale, String formula) {
        return ImmutableRollingFormulaSpec.builder()
                .revWindowScale(revWindowScale)
                .fwdWindowScale(fwdWindowScale)
                .formula(formula)
                .build();
    }

    /**
     * If {@link #paramToken()} is not supplied, this will contain a {@link Selectable} of the parsed
     * {@link #formula()}.
     */
    @Value.Lazy
    public Selectable selectable() {
        if (paramToken().isPresent()) {
            throw new UnsupportedOperationException("selectable() is not supported when paramToken() is present");
        }
        return Selectable.parse(formula());
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
        if (!paramToken().isPresent()) {
            // Call the selectable method to parse now and throw on invalid input.
            selectable();
        }
    }
}
