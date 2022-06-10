package io.deephaven.engine.table.updateBySpec;

import org.jetbrains.annotations.NotNull;

/**
 * A {@link UpdateBySpec} for performing a forward fill of the specified columns.
 */
public class FillBySpec implements UpdateBySpec {
    public static FillBySpec of() {
        return new FillBySpec();
    }

    @NotNull
    @Override
    public String describe() {
        return "ForwardFill";
    }

    @Override
    public boolean applicableTo(@NotNull Class<?> inputType) {
        return true;
    }

    @Override
    public <V extends Visitor> V walk(final @NotNull V v) {
        v.visit(this);
        return v;
    }
}
