package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class FilterQuick extends FilterBase {

    public static FilterQuick of(ColumnName column, String quickSearch) {
        return ImmutableFilterQuick.of(column, quickSearch);
    }

    @Parameter
    public abstract ColumnName column();

    @Parameter
    public abstract String quickSearch();

    @Override
    public final FilterNot<FilterQuick> invert() {
        return FilterNot.of(this);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
