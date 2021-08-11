package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Evaluates to {@code true} when the given {@link #column() column} value is not {@code null}.
 */
@Immutable
@SimpleStyle
public abstract class FilterIsNotNull extends FilterBase {

    public static FilterIsNotNull of(ColumnName column) {
        return ImmutableFilterIsNotNull.of(column);
    }

    /**
     * The column name.
     *
     * @return the column name
     */
    @Parameter
    public abstract ColumnName column();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
