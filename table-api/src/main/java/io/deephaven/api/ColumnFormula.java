package io.deephaven.api;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnFormula implements Selectable {

    public static ColumnFormula of(ColumnName columnName, Expression expression) {
        return ImmutableColumnFormula.of(columnName, expression);
    }

    public static ColumnFormula parse(String x) {
        final int ix = x.indexOf('=');
        if (ix < 0 || ix + 1 == x.length()) {
            throw new IllegalArgumentException(String.format("Unable to parse '%s'", x));
        }
        if (x.charAt(ix + 1) == '=') {
            throw new IllegalArgumentException(String.format("Unable to parse '%s'", x));
        }
        return of(ColumnName.parse(x.substring(0, ix)), Expression.parse(x.substring(ix + 1)));
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
