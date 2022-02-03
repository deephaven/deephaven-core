package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Specifies an aggregation that outputs the sum of the input column values weighted by the {@link #weight() weight
 * column} values for each group. Only works for numeric input types.
 *
 * @see TableOperations#wsumBy
 */
@Immutable
@SimpleStyle
public abstract class AggSpecWSum extends AggSpecBase {

    public static AggSpecWSum of(ColumnName weight) {
        return ImmutableAggSpecWSum.of(weight);
    }

    @Override
    public final String description() {
        return "sum weighted by " + weight();
    }

    /**
     * Column name for the source of input weights.
     *
     * @return The weight column name
     */
    @Parameter
    public abstract ColumnName weight();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
