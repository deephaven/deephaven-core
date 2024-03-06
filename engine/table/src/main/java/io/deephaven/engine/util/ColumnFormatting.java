//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import org.jetbrains.annotations.NotNull;

/**
 * Container for utilities to work with magic String values used to indicate formatted rows and columns.
 */
public interface ColumnFormatting {

    class Constants {
        private static final String TABLE_STYLE_FORMAT_SUFFIX = "__TABLE_STYLE_FORMAT";
        private static final String TABLE_NUMBER_FORMAT_SUFFIX = "__TABLE_NUMBER_FORMAT";
        private static final String TABLE_DATE_FORMAT_SUFFIX = "__TABLE_DATE_FORMAT";
        private static final String ROW_FORMAT_NAME = "__ROW";
        public static final String ROW_FORMAT_WILDCARD = "*";
        private static final String ROW_STYLE_FORMAT_COLUMN = ROW_FORMAT_NAME + TABLE_STYLE_FORMAT_SUFFIX;
    }

    /**
     * Returns true if this column name is a hidden formatting column.
     *
     * @param columnName the column name to check
     * @return true if the columnName is a formatting column; false otherwise
     */
    static boolean isFormattingColumn(String columnName) {
        return isStyleFormatColumn(columnName) || isNumberFormatColumn(columnName) || isDateFormatColumn(columnName);
    }

    /**
     * @return The name of the row style formatting column
     */
    static String getRowStyleFormatColumn() {
        return Constants.ROW_STYLE_FORMAT_COLUMN;
    }

    /**
     * Returns true if this column name is a hidden row style formatting column.
     *
     * @param columnName the column name to check
     * @return true if the columnName is a row style formatting column; false otherwise
     */
    static boolean isRowStyleFormatColumn(@NotNull final String columnName) {
        return columnName.equals(Constants.ROW_STYLE_FORMAT_COLUMN);
    }

    /**
     * @param baseColumn the column name, or the wildcard name for row formats
     * @return The name of the style formatting column for the specified base column.
     */
    static String getStyleFormatColumn(String baseColumn) {
        return baseColumn.equals(Constants.ROW_FORMAT_WILDCARD)
                ? getRowStyleFormatColumn()
                : baseColumn + Constants.TABLE_STYLE_FORMAT_SUFFIX;
    }

    /**
     * Returns true if this column name is a hidden style formatting column.
     *
     * @param columnName the column name to check
     * @return true if the columnName is a style formatting column; false otherwise
     */
    static boolean isStyleFormatColumn(@NotNull final String columnName) {
        return columnName.endsWith(Constants.TABLE_STYLE_FORMAT_SUFFIX);
    }

    /**
     * @param baseColumn the column name
     * @return The name of the number formatting column for the specified base column.
     */
    static String getNumberFormatColumn(String baseColumn) {
        return baseColumn + Constants.TABLE_NUMBER_FORMAT_SUFFIX;
    }

    /**
     * Returns true if this column name is a hidden number formatting column.
     *
     * @param columnName the column name to check
     * @return true if the columnName is a number formatting column; false otherwise
     */
    static boolean isNumberFormatColumn(@NotNull final String columnName) {
        return columnName.endsWith(Constants.TABLE_NUMBER_FORMAT_SUFFIX);
    }

    /**
     * @param baseColumn the column name
     * @return The name of the date formatting column for the specified base column.
     */
    static String getDateFormatColumn(String baseColumn) {
        return baseColumn + Constants.TABLE_DATE_FORMAT_SUFFIX;
    }

    /**
     * Returns true if this column name is a hidden date formatting column.
     *
     * @param columnName the column name to check
     * @return true if the columnName is a date formatting column; false otherwise
     */
    static boolean isDateFormatColumn(@NotNull final String columnName) {
        return columnName.endsWith(Constants.TABLE_DATE_FORMAT_SUFFIX);
    }

    /**
     * Returns the base column name from a formatting column name.
     *
     * @param columnName the column name
     * @return the base column name formatted by {@code columnName}
     */
    static String getFormatBaseColumn(@NotNull final String columnName) {

        if (columnName.startsWith(Constants.ROW_FORMAT_NAME)) {
            return Constants.ROW_FORMAT_WILDCARD;
        }

        int index;

        index = columnName.lastIndexOf(Constants.TABLE_STYLE_FORMAT_SUFFIX);
        if (index == -1) {
            index = columnName.lastIndexOf(Constants.TABLE_NUMBER_FORMAT_SUFFIX);
            if (index == -1) {
                index = columnName.lastIndexOf(Constants.TABLE_DATE_FORMAT_SUFFIX);
            }
        }
        return index == -1 ? null : columnName.substring(0, index);
    }
}
