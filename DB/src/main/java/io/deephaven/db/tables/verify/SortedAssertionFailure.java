package io.deephaven.db.tables.verify;

import io.deephaven.db.tables.SortingOrder;
import org.jetbrains.annotations.Nullable;

public class SortedAssertionFailure extends TableAssertionFailure {
    SortedAssertionFailure(@Nullable String description, String column, SortingOrder order,
        final String value, final String value2) {
        super("Table violates sorted assertion"
            + (description == null ? "" : ", table description=" + description) + ", column="
            + column + ", " + order + ", " + value + " is out of order with respect to " + value2
            + "!");
    }
}
