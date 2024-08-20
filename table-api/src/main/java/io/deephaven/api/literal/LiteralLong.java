//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.literal;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.util.QueryConstants;
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
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(value());
    }

    @Check
    final void checkNotDeephavenNull() {
        if (value() == QueryConstants.NULL_LONG) {
            throw new IllegalArgumentException(
                    "Can't represent QueryConstants.NULL_LONG, is Deephaven null representation");
        }
    }
}
