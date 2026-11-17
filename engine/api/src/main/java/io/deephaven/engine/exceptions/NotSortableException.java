//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import java.util.Collection;

public class NotSortableException extends RuntimeException {
    public NotSortableException(Collection<String> unsortableColumns, Collection<String> sortableColumns) {
        super(buildErrorMessage(unsortableColumns, sortableColumns));
    }

    private static String buildErrorMessage(Collection<String> unsortableColumns, Collection<String> sortableColumns) {
        final String message = "Sorting is not allowed on this table, but was attempted on: "
                + String.join(", ", unsortableColumns);
        return sortableColumns.isEmpty() ? message
                : message + "; sortable columns: " + String.join(", ", sortableColumns);
    }
}
