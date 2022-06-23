package io.deephaven.api.updateBy.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Min/Max of the specified columns.
 */
@Immutable
@SimpleStyle
public abstract class CumMinMaxSpec implements UpdateBySpec {
    public static CumMinMaxSpec of(boolean isMax) {
        return ImmutableCumMinMaxSpec.of(isMax);
    }

    @Parameter
    public abstract boolean isMax();

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        boolean isPrimitiveNumeric = inputType.equals(double.class) || inputType.equals(float.class)
                || inputType.equals(int.class) || inputType.equals(long.class) || inputType.equals(short.class)
                || inputType.equals(byte.class);
        boolean isBoxedNumeric = Number.class.isAssignableFrom(inputType);
        boolean isBoolean = inputType == boolean.class || inputType == Boolean.class;
        boolean isComparable = Comparable.class.isAssignableFrom(inputType) && inputType != Boolean.class;

        return isPrimitiveNumeric || isBoxedNumeric || isBoolean || isComparable;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
