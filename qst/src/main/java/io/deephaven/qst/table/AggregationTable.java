package io.deephaven.qst.table;

import io.deephaven.api.Selectable;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;

/**
 * @see io.deephaven.api.TableOperations#by(Collection, Collection)
 */
@Immutable
@NodeStyle
public abstract class AggregationTable extends TableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableAggregationTable.builder();
    }

    public abstract TableSpec parent();

    public abstract List<Selectable> columns();

    public abstract List<Aggregation> aggregations();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkNumAggs() {
        if (aggregations().isEmpty()) {
            throw new IllegalArgumentException("Aggregations must not be empty");
        }
    }

    public interface Builder {
        Builder parent(TableSpec parent);

        Builder addColumns(Selectable element);

        Builder addColumns(Selectable... elements);

        Builder addAllColumns(Iterable<? extends Selectable> elements);

        Builder addAggregations(Aggregation element);

        Builder addAggregations(Aggregation... elements);

        Builder addAllAggregations(Iterable<? extends Aggregation> elements);

        AggregationTable build();
    }
}
