//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import javax.annotation.Nullable;

/**
 * Specifies an aggregation that applies a {@link #formula() formula} to each input group (as a Deephaven vector
 * (io.deephaven.vector.Vector)) to produce the corresponding output value. Each input column name is substituted for
 * the {@link #paramToken() param token} for evaluation.
 */
@Immutable
@SimpleStyle
public abstract class AggSpecFormula extends AggSpecBase {
    @Override
    public boolean deferredInputColumns() {
        return isMultiColumnFormula();
    }

    /**
     * Creates a new AggSpecFormula.
     *
     * @param formula the formula
     * @return the AggSpecFormula
     */
    public static AggSpecFormula of(String formula) {
        // Parse the supplied formula for the output column name and formula
        final Selectable column = Selectable.parse(formula);

        final String outputColumnName = column.newColumn().name();
        final String parsedFormula = ((RawString) column.expression()).value();

        return ImmutableAggSpecFormula.of(parsedFormula, null, outputColumnName);
    }

    /**
     * Creates a new AggSpecFormula.
     *
     * @param formula the formula
     * @param paramToken the param token
     * @return the AggSpecFormula
     */
    public static AggSpecFormula of(String formula, String paramToken) {
        return ImmutableAggSpecFormula.of(formula, paramToken, null);
    }

    @Override
    public final String description() {
        if (isMultiColumnFormula()) {
            return "multi-column formula '" + formula() + '\'';
        }
        return "formula '" + formula() + "' with column param '" + paramToken() + '\'';
    }

    /**
     * The formula to use to calculate output values from grouped input values.
     *
     * @return The formula
     */
    @Parameter
    public abstract String formula();

    /**
     * The formula parameter token to be replaced with the input column name for evaluation.
     * 
     * @return The parameter token
     */
    @Parameter
    @Nullable
    public abstract String paramToken();

    /**
     * Will return the output column name parsed from the supplied formula (or {@code null} if this is a single-column
     * formula applied to many output columns).
     *
     * @return The output column name (or null if this is not a multi-column formula)
     */
    @Parameter
    @Nullable
    public abstract String outputColumnName();

    /**
     * Whether this is a multi-column input formula or if it is a single-column input formula with a parameter token.
     */
    public boolean isMultiColumnFormula() {
        return outputColumnName() != null;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkFormula() {
        if (formula().isEmpty()) {
            throw new IllegalArgumentException("formula must not be empty");
        }
    }
}
