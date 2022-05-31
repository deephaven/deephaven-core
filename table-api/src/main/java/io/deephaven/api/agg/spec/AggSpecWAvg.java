package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Specifies an aggregation that outputs the average (arithmetic mean) of the input column values weighted by the
 * {@link #weight() weight column} values for each group. Only works for numeric input types.
 *
 * @see TableOperations#wavgBy
 */
@Immutable
@SimpleStyle
public abstract class AggSpecWAvg extends AggSpecBase {

    public static AggSpecWAvg of(ColumnName weight) {
        return ImmutableAggSpecWAvg.of(weight);
    }

    @Override
    public final String description() {
        return "average weighted by " + weight();
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
