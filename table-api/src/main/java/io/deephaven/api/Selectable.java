//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.api.expression.Expression;

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
public interface Selectable extends ConcurrencyControl<Selectable> {

    static Selectable of(ColumnName newColumn, Expression expression) {
        if (newColumn.equals(expression)) {
            return newColumn;
        }
        return SelectableImpl.of(newColumn, expression);
    }

    static Selectable parse(String x) {
        final int ix = x.indexOf('=');
        if (ix < 0) {
            return ColumnName.parse(x);
        }
        if (ix + 1 == x.length()) {
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

    /**
     * @return the barriers this selectable respects
     */
    default Object[] respectedBarriers() {
        return null;
    }

    /**
     * @return the barriers this selectable declares
     */
    default Object[] declaredBarriers() {
        return null;
    }

    /**
     * @return null if this Selectable has no opinion on whether it is serial (so the system default should be used),
     *         false if this Selectable can be reordered within a column, or true if this Selectable must be evaluated
     *         in order
     */
    default Boolean isSerial() {
        return null;
    }

    @Override
    default Selectable withSerial() {
        if (isSerial() != null && isSerial()) {
            return this;
        }
        return new SelectableWithSerial(this);
    }

    @Override
    default Selectable withDeclaredBarriers(Object... declaredBarriers) {
        return new SelectableWithDeclaredBarriers(this, Arrays.stream(declaredBarriers));
    }

    @Override
    default Selectable withRespectedBarriers(Object... respectedBarriers) {
        return new SelectableWithRespectedBarriers(this, Arrays.stream(respectedBarriers));
    }
}
