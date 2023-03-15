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
abstract class LiteralLong extends LiteralBase {

    public static LiteralLong of(long value) {
        return ImmutableLiteralLong.of(value);
    }

    @Parameter
    public abstract long value();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(value());
        return visitor;
    }

    @Check
    final void checkNotDeephavenNull() {
        if (value() == Long.MIN_VALUE) {
            throw new IllegalArgumentException(
                    "Can't represent Long.MIN_VALUE, is Deephaven null representation");
        }
    }
}
