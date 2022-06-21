package io.deephaven.engine.table.updateBySpec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.util.type.TypeUtils;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;
import org.jetbrains.annotations.NotNull;

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
    public final boolean applicableTo(@NotNull Class<?> inputType) {
        return TypeUtils.isNumeric(inputType)
                || (Comparable.class.isAssignableFrom(inputType) && inputType != Boolean.class);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
