//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.literal;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.util.QueryConstants;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
abstract class LiteralByte extends LiteralBase {

    public static LiteralByte of(byte value) {
        return ImmutableLiteralByte.of(value);
    }

    @Parameter
    public abstract byte value();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(value());
    }

    @Check
    final void checkNotDeephavenNull() {
        if (value() == QueryConstants.NULL_BYTE) {
            throw new IllegalArgumentException(
                    "Can't represent QueryConstants.NULL_BYTE, is Deephaven null representation");
        }
    }
}
