package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterNot;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.io.Serializable;

/**
 * An un-parsed string; used for cases where the server has string-parsing that hasn't been structurally represented at
 * the api layer yet.
 */
@Immutable
@SimpleStyle
public abstract class RawString implements Expression, Filter, Serializable {

    public static RawString of(String x) {
        return ImmutableRawString.of(x);
    }

    @Parameter
    public abstract String value();

    @Override
    public final FilterNot not() {
        return FilterNot.of(this);
    }

    @Override
    public final <V extends Expression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final <V extends Filter.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
