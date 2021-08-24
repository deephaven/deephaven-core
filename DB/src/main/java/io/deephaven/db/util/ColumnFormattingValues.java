package io.deephaven.db.util;

/**
 * Container for magic String values used to indicate formatted rows
 */
public interface ColumnFormattingValues {
    String ROW_FORMAT_NAME = "__ROWFORMATTED";
    String TABLE_FORMAT_NAME = "__WTABLE_FORMAT";
    String TABLE_NUMERIC_FORMAT_NAME = "__WTABLE_NUM_FORMAT";
    String TABLE_DATE_FORMAT_NAME = "__WTABLE_DATE_FORMAT";

    /**
     * Returns true if this column name is a hidden formatting column.
     *
     * @param columnName the column name to check
     * @return true if the columnName is a formatting column; false otherwise
     */
    static boolean isFormattingColumn(String columnName) {
        return columnName.endsWith(TABLE_FORMAT_NAME)
            || columnName.endsWith(TABLE_NUMERIC_FORMAT_NAME)
            || columnName.endsWith(TABLE_DATE_FORMAT_NAME);
    }

    /**
     * @param baseColumn The column.
     * @return The name of the number formatting column for the specified base column.
     */
    static String getNumberFormatColumn(String baseColumn) {
        return baseColumn + TABLE_NUMERIC_FORMAT_NAME;
    }

    /**
     * @param baseColumn The column.
     * @return The name of the Date formatting column for the specified base column.
     */
    static String getDateFormatColumn(String baseColumn) {
        return baseColumn + TABLE_DATE_FORMAT_NAME;
    }
}
