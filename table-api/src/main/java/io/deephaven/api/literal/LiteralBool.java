//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.literal;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.filter.Filter;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
abstract class LiteralBool extends LiteralBase implements LiteralFilter {

    public static LiteralBool of(boolean value) {
        return ImmutableLiteralBool.of(value);
    }

    @Parameter
    public abstract boolean value();

    @Override
    public final LiteralBool invert() {
        return of(!value());
    }

    @Override
    public final <T> T walk(Literal.Visitor<T> visitor) {
        return visitor.visit(value());
    }

    @Override
    public final <T> T walk(Filter.Visitor<T> visitor) {
        return visitor.visit(value());
    }
}
