package io.deephaven.api.agg;

import io.deephaven.api.JoinAddition;
import io.deephaven.api.SortColumn;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
public abstract class SortedLast implements Aggregation {

    public abstract JoinAddition addition();

    public abstract List<SortColumn> columns();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Value.Check
    final void nonEmptyColumns() {
        if (columns().isEmpty()) {
            throw new IllegalArgumentException("columns() must be non-empty");
        }
    }
}
