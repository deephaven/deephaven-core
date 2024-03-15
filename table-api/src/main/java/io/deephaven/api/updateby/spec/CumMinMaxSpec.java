//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Min/Max of the specified columns.
 */
@Immutable
@SimpleStyle
public abstract class CumMinMaxSpec extends UpdateBySpecBase {
    public static CumMinMaxSpec of(boolean isMax) {
        return ImmutableCumMinMaxSpec.of(isMax);
    }

    @Parameter
    public abstract boolean isMax();

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return
        // is primitive or boxed numeric?
        applicableToNumeric(inputType)

                // is comparable?
                || (Comparable.class.isAssignableFrom(inputType) && inputType != Boolean.class);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
