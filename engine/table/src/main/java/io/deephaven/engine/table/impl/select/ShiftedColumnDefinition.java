//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import com.google.common.base.Objects;
import org.jetbrains.annotations.NotNull;

public class ShiftedColumnDefinition {
    private final String columnName;
    private final long shiftAmount;

    public ShiftedColumnDefinition(
            final @NotNull String columnName,
            final long shiftAmount) {
        this.columnName = columnName;
        this.shiftAmount = shiftAmount;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getResultColumnName() {
        return String.format("__%s_Shifted_%s_%d__",
                columnName,
                shiftAmount < 0 ? "Minus" : "Plus",
                Math.abs(shiftAmount));
    }

    public long getShiftAmount() {
        return shiftAmount;
    }

    @Override
    public String toString() {
        return "ShiftedColumnDefinition{" +
                "columnName=" + columnName +
                ", shiftAmount=" + shiftAmount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ShiftedColumnDefinition))
            return false;

        ShiftedColumnDefinition that = (ShiftedColumnDefinition) o;

        if (shiftAmount != that.shiftAmount)
            return false;
        return columnName.equals(that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columnName, shiftAmount);
    }
}
