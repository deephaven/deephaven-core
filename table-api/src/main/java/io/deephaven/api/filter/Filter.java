package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;

import java.util.Collection;

/**
 * Represents an evaluate-able filter.
 *
 * @see io.deephaven.api.TableOperations#where(Collection)
 */
public interface Filter {
    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName name);

        void visit(FilterMatch match);

        void visit(RawString rawString);
    }
}
