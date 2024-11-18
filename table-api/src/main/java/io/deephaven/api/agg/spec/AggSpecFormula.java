//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Specifies an aggregation that applies a {@link #formula() formula} to each input group (as a Deephaven vector
 * (io.deephaven.vector.Vector)) to produce the corresponding output value. Each input column name is substituted for
 * the {@link #paramToken() param token} for evaluation.
 */
@Immutable
@SimpleStyle
@Deprecated
public abstract class AggSpecFormula extends AggSpecBase {

    public static final String PARAM_TOKEN_DEFAULT = "each";

    /**
     * Creates a new AggSpecFormula with {@code paramToken} of {@value PARAM_TOKEN_DEFAULT}.
     *
     * <p>
     * todo The
     *
     * @param formula the formula
     * @return the AggSpecFormula
     */
    public static AggSpecFormula of(String formula) {
        return of(formula, PARAM_TOKEN_DEFAULT);
    }

    /**
     * Creates a new AggSpecFormula.
     *
     * @param formula the formula
     * @param paramToken the param token
     * @return the AggSpecFormula
     */
    public static AggSpecFormula of(String formula, String paramToken) {
        return ImmutableAggSpecFormula.of(formula, paramToken);
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
    @Parameter
    public abstract String formula();

    /**
     * The formula parameter token to be replaced with the input column name for evaluation.
     * 
     * @return The parameter token
     */
    @Parameter
    public abstract String paramToken();

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

    @Check
    final void checkParamToken() {
        if (paramToken().isEmpty()) {
            throw new IllegalArgumentException("paramToken must not be empty");
        }
    }
}
