/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.value;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
abstract class LiteralInt extends LiteralBase {

    public static LiteralInt of(int value) {
        return ImmutableLiteralInt.of(value);
    }

    @Parameter
    public abstract int value();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(value());
        return visitor;
    }

    @Check
    final void checkNotDeephavenNull() {
        if (value() == Integer.MIN_VALUE) {
            throw new IllegalArgumentException(
                    "Can't represent Integer.MIN_VALUE, is Deephaven null representation");
        }
    }
}
