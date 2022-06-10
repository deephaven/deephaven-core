package io.deephaven.engine.table.updateBySpec;

import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Product of the specified columns.
 */
public class CumProdSpec implements UpdateBySpec {
    public static CumProdSpec of() {
        return new CumProdSpec();
    }

    @NotNull
    @Override
    public String describe() {
        return "CumProd";
    }

    @Override
    public boolean applicableTo(@NotNull Class<?> inputType) {
        return TypeUtils.isNumeric(inputType);
    }

    @Override
    public <V extends Visitor> V walk(@NotNull V v) {
        v.visit(this);
        return v;
    }
}
