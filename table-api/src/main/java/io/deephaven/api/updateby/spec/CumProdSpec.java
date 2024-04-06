//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Product of the specified columns.
 */
@Immutable
@SingletonStyle
public abstract class CumProdSpec extends UpdateBySpecBase {
    public static CumProdSpec of() {
        return ImmutableCumProdSpec.of();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        // is primitive or boxed numeric?
        return applicableToNumeric(inputType);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
