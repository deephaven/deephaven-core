package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that applies a {@link #formula() formula} to each input group (as a Deephaven vector
 * (io.deephaven.vector.Vector)) to produce the corresponding output value. Each input column name is substituted for
 * the {@link #paramToken() param token} for evaluation.
 */
@Immutable
@BuildableStyle
public abstract class AggSpecFormula extends AggSpecBase {

    public static AggSpecFormula of(String formula) {
        return ImmutableAggSpecFormula.builder().formula(formula).build();
    }

    public static AggSpecFormula of(String formula, String paramToken) {
        return ImmutableAggSpecFormula.builder().formula(formula).paramToken(paramToken).build();
    }

    @Override
    public final String description() {
        return "formula '" + formula() + "' with column param '" + paramToken() + '\'';
    }

    /**
     * The formula to use to calculate output values from grouped input values.
     *
     * @return The formula
     */
    public abstract String formula();

    /**
     * The formula parameter token to be replaced with the input column name for evaluation.
     * 
     * @return The parameter token
     */
    @Default
    public String paramToken() {
        return "each";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
