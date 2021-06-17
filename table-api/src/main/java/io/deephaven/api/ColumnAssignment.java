package io.deephaven.api;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnAssignment implements JoinAddition {

    public static ColumnAssignment of(ColumnName newColumn, ColumnName existingColumn) {
        return ImmutableColumnAssignment.of(newColumn, existingColumn);
    }

    public static ColumnAssignment parse(String x) {
        final int ix = x.indexOf('=');
        if (ix < 0) {
            throw new IllegalArgumentException(String.format("Unable to parse '%s'", x));
        }
        return of(ColumnName.of(x.substring(0, ix)), ColumnName.of(x.substring(ix + 1)));
    }

    @Parameter
    public abstract ColumnName newColumn();

    @Parameter
    public abstract ColumnName existingColumn();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
