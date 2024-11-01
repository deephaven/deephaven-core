//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Count of non-null values in the specified columns.
 */
@Immutable
@SimpleStyle
public abstract class CumCountSpec extends UpdateBySpecBase {
    /**
     * The types of counts that can be performed.
     */
    public enum CumCountType {
        NON_NULL, NULL, NEGATIVE, POSITIVE, ZERO, NAN, INFINITE, FINITE
    }

    public static CumCountSpec of(final CumCountType countType) {
        return ImmutableCumCountSpec.of(countType);
    }

    @Value.Parameter
    public abstract CumCountType countType();

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        if (countType() == CumCountType.NULL || countType() == CumCountType.NON_NULL) {
            // null/non-null applies to all types
            return true;
        }
        return applicableToNumeric(inputType)
                || inputType == char.class || inputType == Character.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
