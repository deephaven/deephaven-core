package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import org.jetbrains.annotations.NotNull;

/**
 * Encapsulate some hacky conventions we use to "standardize" absolute value sorting directives from the UI. See
 * server/src/main/java/io/deephaven/server/table/ops/SortTableGrpcImpl.java for the other side of this. The convention
 * has implications for sort restrictions.
 */
public class AbsoluteSortColumnConventions {

    private static final String ABSOLUTE_VALUE_PREFIX = "__ABS__";

    public static boolean isAbsoluteColumnName(@NotNull final String columnName) {
        return columnName.startsWith(ABSOLUTE_VALUE_PREFIX);
    }

    public static String stripAbsoluteColumnName(@NotNull final String columnName) {
        return isAbsoluteColumnName(columnName) ? absoluteColumnNameToBaseName(columnName) : columnName;
    }

    public static String baseColumnNameToAbsoluteName(@NotNull final String columnName) {
        return ABSOLUTE_VALUE_PREFIX + columnName;
    }

    public static String absoluteColumnNameToBaseName(@NotNull final String columnName) {
        return columnName.substring(ABSOLUTE_VALUE_PREFIX.length());
    }

    public static Selectable makeSelectable(
            @NotNull final String absoluteColumnName,
            @NotNull final String baseColumnName) {
        return FormulaColumn.createFormulaColumn(absoluteColumnName, "abs(" + baseColumnName + ")");
    }
}
