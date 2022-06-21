package io.deephaven.engine.table.updateBySpec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.util.type.TypeUtils;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;

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
    public final boolean applicableTo(@NotNull Class<?> inputType) {
        return TypeUtils.isNumeric(inputType);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
