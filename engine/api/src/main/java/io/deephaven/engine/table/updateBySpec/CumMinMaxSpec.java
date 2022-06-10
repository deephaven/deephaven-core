package io.deephaven.engine.table.updateBySpec;

import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Min/Max of the specified columns.
 */
public class CumMinMaxSpec implements UpdateBySpec {
    private final boolean isMax;

    public static CumMinMaxSpec of(boolean isMax) {
        return new CumMinMaxSpec(isMax);
    }

    public CumMinMaxSpec(boolean isMax) {
        this.isMax = isMax;
    }

    public boolean isMax() {
        return isMax;
    }

    @NotNull
    @Override
    public String describe() {
        return isMax ? "CumMax" : "CumMin";
    }

    @Override
    public boolean applicableTo(@NotNull Class<?> inputType) {
        return TypeUtils.isNumeric(inputType) || (Comparable.class.isAssignableFrom(inputType) && inputType != Boolean.class);
    }

    @Override
    public <V extends Visitor> V walk(@NotNull V v) {
        v.visit(this);
        return v;
    }
}
