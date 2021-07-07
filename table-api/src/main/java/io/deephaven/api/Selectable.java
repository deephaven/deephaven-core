package io.deephaven.api;

import io.deephaven.api.expression.Expression;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

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
@Immutable
@SimpleStyle
public abstract class Selectable {

    public static Selectable of(ColumnName newAndExisting) {
        return of(newAndExisting, newAndExisting);
    }

    public static Selectable of(ColumnName newColumn, Expression expression) {
        return ImmutableSelectable.of(newColumn, expression);
    }

    public static Selectable parse(String x) {
        if (ColumnName.isValidParsedColumnName(x)) {
            return of(ColumnName.parse(x));
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
        return of(ColumnName.parse(x.substring(0, ix)), RawString.of(x.substring(ix + 1)));
    }

    public static List<Selectable> from(String... values) {
        return from(Arrays.asList(values));
    }

    public static List<Selectable> from(Collection<String> values) {
        return values.stream().map(Selectable::parse).collect(Collectors.toList());
    }

    /**
     * The new column name, to be added to the new table.
     *
     * @return the new column name
     */
    @Parameter
    public abstract ColumnName newColumn();

    /**
     * The expression.
     *
     * @return the expression
     */
    @Parameter
    public abstract Expression expression();
}
