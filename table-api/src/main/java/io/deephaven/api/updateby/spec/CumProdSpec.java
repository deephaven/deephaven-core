package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Product of the specified columns.
 */
@Immutable
@SimpleStyle
public abstract class CumProdSpec extends UpdateBySpecBase {
    public static CumProdSpec of() {
        return ImmutableCumProdSpec.of();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return
        // is primitive numeric?
        inputType == double.class || inputType == float.class
                || inputType == int.class || inputType == long.class || inputType == short.class
                || inputType == byte.class

                // is boxed numeric?
                || Number.class.isAssignableFrom(inputType);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
