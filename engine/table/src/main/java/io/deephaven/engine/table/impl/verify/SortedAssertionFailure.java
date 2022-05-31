package io.deephaven.engine.table.impl.verify;

import io.deephaven.engine.table.impl.SortingOrder;
import org.jetbrains.annotations.Nullable;

public class SortedAssertionFailure extends TableAssertionFailure {
    SortedAssertionFailure(@Nullable String description, String column, SortingOrder order, final String value,
            final String value2) {
        super("Table violates sorted assertion" + (description == null ? "" : ", table description=" + description)
                + ", column=" + column + ", " + order + ", " + value + " is out of order with respect to " + value2
                + "!");
    }
}
