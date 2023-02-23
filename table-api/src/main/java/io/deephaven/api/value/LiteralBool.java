/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.value;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.Filter.Visitor;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
abstract class LiteralBool extends LiteralBase implements Filter {

    public static LiteralBool of(boolean value) {
        return ImmutableLiteralBool.of(value);
    }

    @Parameter
    public abstract boolean value();

    @Override
    public final Filter inverse() {
        return of(!value());
    }

    @Override
    public final <V extends Literal.Visitor> V walk(V visitor) {
        visitor.visit(value());
        return visitor;
    }

    @Override
    public <V extends Filter.Visitor> V walk(V visitor) {
        visitor.visit(value());
        return visitor;
    }
}
