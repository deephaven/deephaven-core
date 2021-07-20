package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;

import java.io.Serializable;
import java.util.Collection;

/**
 * Represents an evaluate-able filter.
 *
 * @see io.deephaven.api.TableOperations#where(Collection)
 */
public interface Filter extends Serializable {

    static FilterIsNull isNull(ColumnName column) {
        return FilterIsNull.of(column);
    }

    static FilterIsNotNull isNotNull(ColumnName column) {
        return FilterIsNotNull.of(column);
    }

    static FilterNot not(Filter filter) {
        return FilterNot.of(filter);
    }

    FilterNot not();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        // TODO (deephaven-core#829): Add more table api Filter structuring

        void visit(FilterIsNull isNull);

        void visit(FilterIsNotNull isNotNull);

        void visit(FilterCondition condition);

        void visit(FilterNot not);

        void visit(RawString rawString);
    }
}
