package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.stream.Collectors;

@Immutable
@BuildableStyle
public abstract class AggSpecFormula extends AggSpecBase {

    public static AggSpecFormula of(String formula) {
        return ImmutableAggSpecFormula.builder().formula(formula).build();
    }

    public static AggSpecFormula of(String formula, String formulaParam) {
        return ImmutableAggSpecFormula.builder().formula(formula).formulaParam(formulaParam).build();
    }

    @Override
    public final String description() {
        return "formula '" + formula() + "' with column param '" + formulaParam() + '\'';
    }

    public abstract String formula();

    @Default
    public String formulaParam() {
        return "each";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
