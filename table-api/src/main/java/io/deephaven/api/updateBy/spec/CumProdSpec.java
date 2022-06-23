package io.deephaven.api.updateBy.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Product of the specified columns.
 */
@Immutable
@SimpleStyle
public abstract class CumProdSpec implements UpdateBySpec {
    public static CumProdSpec of() {
        return ImmutableCumProdSpec.of();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        boolean isPrimitiveNumeric = inputType.equals(double.class) || inputType.equals(float.class)
                || inputType.equals(int.class) || inputType.equals(long.class) || inputType.equals(short.class)
                || inputType.equals(byte.class);
        boolean isBoxedNumeric = Number.class.isAssignableFrom(inputType);

        return isPrimitiveNumeric || isBoxedNumeric;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
