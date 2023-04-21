package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class FilterQuick extends FilterBase {

    public static FilterQuick of(Expression expression, String quickSearch) {
        return ImmutableFilterQuick.of(expression, quickSearch);
    }

    @Parameter
    public abstract Expression expression();

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
