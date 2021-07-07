package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.io.Serializable;

@Immutable
@SimpleStyle
public abstract class FilterMatch implements Filter, Serializable {

    public static FilterMatch of(ColumnName left, ColumnName right) {
        return ImmutableFilterMatch.of(left, right);
    }

    public static FilterMatch parse(String x) {
        final int ix = x.indexOf('=');
        if (ix < 0 || ix + 1 == x.length()) {
            throw new IllegalArgumentException(String.format(
                "Unable to parse match '%s', expected form '<left>==<right>' or `<left>=<right>`",
                x));
        }
        final int ix2 = x.charAt(ix + 1) == '=' ? ix + 1 : ix;
        if (ix2 + 1 == x.length()) {
            throw new IllegalArgumentException(String.format(
                "Unable to parse match '%s', expected form '<left>==<right>' or `<left>=<right>`",
                x));
        }
        ColumnName left = ColumnName.parse(x.substring(0, ix));
        ColumnName right = ColumnName.parse(x.substring(ix2 + 1));
        return of(left, right);
    }

    @Parameter
    public abstract ColumnName left();

    @Parameter
    public abstract ColumnName right();

    @Override
    public final <V extends Filter.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
