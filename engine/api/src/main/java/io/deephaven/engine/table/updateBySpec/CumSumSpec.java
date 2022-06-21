package io.deephaven.engine.table.updateBySpec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.util.type.TypeUtils;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Sum of the specified columns.
 */
@Immutable
@SimpleStyle
public class CumSumSpec implements UpdateBySpec {
    public static CumSumSpec of() {
        return ImmutableCumSumSpec.of();
    }

    @Override
    public final boolean applicableTo(@NotNull Class<?> inputType) {
        return TypeUtils.isNumeric(inputType) || inputType == boolean.class || inputType == Boolean.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
