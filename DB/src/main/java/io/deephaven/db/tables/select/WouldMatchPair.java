package io.deephaven.db.tables.select;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.v2.select.SelectFilter;

import java.util.Objects;

/**
 * A Pair of (Column name, Filter) for use with {@link Table#wouldMatch(String...)}.
 */
public class WouldMatchPair {
    public static final WouldMatchPair[] ZERO_LENGTH_WOULD_MATCH_PAIR_ARRAY = new WouldMatchPair[0];

    private final String columnName;
    private final SelectFilter filter;

    /**
     * Create a SelectPair from a column name and DB expression.
     *
     * @param columnName the name of the resultant column
     * @param expression the expression to populate the column
     */
    public WouldMatchPair(String columnName, String expression) {
        this(columnName, SelectFilterFactory.getExpression(expression));
    }

    /**
     * Create a SelectPair from a column name and {@link SelectFilter}
     *
     * @see SelectFilterFactory
     *
     * @param columnName the name of the resultant column
     * @param filter the filter to populate the column
     */
    public WouldMatchPair(String columnName, SelectFilter filter) {
        this.columnName = NameValidator.validateColumnName(columnName);
        this.filter = filter;
    }

    public String getColumnName() {
        return columnName;
    }

    public SelectFilter getFilter() {
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final WouldMatchPair other = (WouldMatchPair) o;
        return Objects.equals(columnName, other.columnName) &&
                Objects.equals(filter, other.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, filter);
    }
}
