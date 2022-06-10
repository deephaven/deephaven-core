package io.deephaven.engine.table.updateBySpec;

import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Sum of the specified columns.
 */
public class CumSumSpec implements UpdateBySpec {
    public static CumSumSpec of() {
        return new CumSumSpec();
    }

    @NotNull
    @Override
    public String describe() {
        return "CumSum";
    }

    @Override
    public boolean applicableTo(@NotNull Class<?> inputType) {
        return TypeUtils.isNumeric(inputType) || inputType == boolean.class || inputType == Boolean.class;
    }

    @Override
    public <V extends Visitor> V walk(@NotNull V v) {
        v.visit(this);
        return v;
    }
}
