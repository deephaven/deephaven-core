//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterNot;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * An un-parsed string; used for cases where the server has string-parsing that hasn't been structurally represented at
 * the api layer yet.
 */
@Immutable
@SimpleStyle
public abstract class RawString implements Expression, Filter {

    public static RawString of(String x) {
        return ImmutableRawString.of(x);
    }

    @Parameter
    public abstract String value();

    @Override
    public final FilterNot<RawString> invert() {
        return Filter.not(this);
    }

    @Override
    public final <T> T walk(Expression.Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final <T> T walk(Filter.Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
