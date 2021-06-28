package io.deephaven.api;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnMatch implements JoinMatch, Filter {

    public static ColumnMatch of(ColumnName left, ColumnName right) {
        return ImmutableColumnMatch.of(left, right);
    }

    public static ColumnMatch parse(String x) {
        final int ix = x.indexOf('=');
        if (ix < 0 || ix + 1 == x.length()) {
            throw new IllegalArgumentException(String.format("Unable to parse '%s'", x));
        }
        final int ix2 = x.charAt(ix + 1) == '=' ? ix + 1 : ix;
        return of(ColumnName.parse(x.substring(0, ix)), ColumnName.parse(x.substring(ix2 + 1)));
    }

    @Parameter
    public abstract ColumnName left();

    @Parameter
    public abstract ColumnName right();

    @Override
    public final <V extends JoinMatch.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final <V extends Filter.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
