package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Sum of the specified columns.
 */
@Immutable
@SimpleStyle
public class CumSumSpec extends UpdateBySpecBase {
    public static CumSumSpec of() {
        return ImmutableCumSumSpec.of();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return
        // is primitive numeric?
        inputType == double.class || inputType == float.class
                || inputType == int.class || inputType == long.class || inputType == short.class
                || inputType == byte.class

                // is boxed numeric?
                || Number.class.isAssignableFrom(inputType)

                // is boolean?
                || inputType == boolean.class || inputType == Boolean.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
