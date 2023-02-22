/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.value;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
abstract class LiteralBool extends LiteralBase {

    public static LiteralBool of(boolean value) {
        return ImmutableLiteralBool.of(value);
    }

    @Parameter
    public abstract boolean value();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(value());
        return visitor;
    }
}
