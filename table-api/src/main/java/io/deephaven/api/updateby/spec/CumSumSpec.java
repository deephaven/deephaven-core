//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Sum of the specified columns.
 */
@Immutable
@SingletonStyle
public class CumSumSpec extends UpdateBySpecBase {
    public static CumSumSpec of() {
        return ImmutableCumSumSpec.of();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return
        // is primitive or boxed numeric?
        applicableToNumeric(inputType)
                || inputType == char.class || inputType == Character.class
                // is boolean?
                || inputType == boolean.class || inputType == Boolean.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
