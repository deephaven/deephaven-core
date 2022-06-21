package io.deephaven.engine.table.updateBySpec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link UpdateBySpec} for performing a forward fill of the specified columns.
 */

@Immutable
@SimpleStyle
public abstract class FillBySpec implements UpdateBySpec {
    public static FillBySpec of() {
        return ImmutableFillBySpec.of();
    }

    @Override
    public final boolean applicableTo(@NotNull Class<?> inputType) {
        return true;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
