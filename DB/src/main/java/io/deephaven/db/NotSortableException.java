package io.deephaven.db;

import io.deephaven.base.StringUtils;

import java.util.Collection;

public class NotSortableException extends RuntimeException {
    public NotSortableException(Collection<String> unsortableColumns,
        Collection<String> sortableColumns) {
        super(buildErrorMessage(unsortableColumns, sortableColumns));
    }

    private static String buildErrorMessage(Collection<String> unsortableColumns,
        Collection<String> sortableColumns) {
        String message = sortableColumns.isEmpty() ? "Sorting is not allowed on this table"
            : "Sorting is not allowed on this table, but was attempted on: "
                + StringUtils.joinStrings(unsortableColumns, ", ");

        message += " but was attempted on: " + StringUtils.joinStrings(unsortableColumns, ", ");

        return message;
    }
}
