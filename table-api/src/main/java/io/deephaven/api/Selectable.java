package io.deephaven.api;

import io.deephaven.api.expression.Expression;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a selectable assignment for an {@link Expression}.
 *
 * @see TableOperations#view(Collection)
 * @see TableOperations#update(Collection)
 * @see TableOperations#updateView(Collection)
 * @see TableOperations#select(Collection)
 */
public interface Selectable extends Serializable {

    static Selectable of(ColumnName newColumn, Expression expression) {
        if (newColumn.equals(expression)) {
            return newColumn;
        }
        return SelectableImpl.of(newColumn, expression);
    }

    static Selectable parse(String x) {
        if (ColumnName.isValidParsedColumnName(x)) {
            return ColumnName.parse(x);
        }
        final int ix = x.indexOf('=');
        if (ix < 0 || ix + 1 == x.length()) {
            throw new IllegalArgumentException(String.format(
                "Unable to parse formula '%s', expected form '<newColumn>=<expression>'", x));
        }
        if (x.charAt(ix + 1) == '=') {
            throw new IllegalArgumentException(String.format(
                "Unable to parse formula '%s', expected form '<newColumn>=<expression>'", x));
        }
        return SelectableImpl.of(ColumnName.parse(x.substring(0, ix)),
            RawString.of(x.substring(ix + 1)));
    }

    static List<Selectable> from(String... values) {
        return from(Arrays.asList(values));
    }

    static List<Selectable> from(Collection<String> values) {
        return values.stream().map(Selectable::parse).collect(Collectors.toList());
    }

    /**
     * The new column name, to be added to the new table.
     *
     * @return the new column name
     */
    ColumnName newColumn();

    /**
     * The expression.
     *
     * @return the expression
     */
    Expression expression();
}
