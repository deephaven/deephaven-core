package io.deephaven.qst.table;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnFormula implements Selectable {

    public static ColumnFormula parse(String x) {
        final int eqIx = x.indexOf('=');
        return ImmutableColumnFormula.of(ImmutableColumnName.of(x.substring(0, eqIx)),
            Expression.parse(x.substring(eqIx + 1)));
    }

    @Parameter
    public abstract ColumnName newColumn();

    @Parameter
    public abstract Expression expression();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
