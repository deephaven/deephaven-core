package io.deephaven.qst.table;

import io.deephaven.qst.table.agg.Aggregation;
import java.util.List;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class AggregationTable extends TableBase implements SingleParentTable {

    public abstract Table parent();

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
}
