package io.deephaven.api;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterMatch;

import java.util.Objects;

/**
 * A set of static helpers to turn strongly-typed api arguments into their {@link String}
 * counterparts.
 */
public class Strings {

    public static String of(ColumnName columnName) {
        return columnName.name();
    }

    public static String of(RawString rawString) {
        return rawString.value();
    }

    public static String of(FilterMatch match) {
        return String.format("%s==%s", of(match.left()), of(match.right()));
    }

    public static String of(Pair pair) {
        if (pair.input().equals(pair.output())) {
            return of(pair.output());
        }
        return String.format("%s=%s", of(pair.output()), of(pair.input()));
    }

    public static String of(JoinMatch match) {
        if (match.left().equals(match.right())) {
            return of(match.left());
        }
        return String.format("%s==%s", of(match.left()), of(match.right()));
    }

    public static String of(JoinAddition addition) {
        if (addition.newColumn().equals(addition.existingColumn())) {
            return of(addition.newColumn());
        }
        return String.format("%s=%s", of(addition.newColumn()), of(addition.existingColumn()));
    }

    public static String of(Selectable selectable) {
        if (selectable.newColumn().equals(selectable.expression())) {
            return selectable.newColumn().name();
        }
        String rhs = selectable.expression().walk(new UniversalAdapter()).getOut();
        return String.format("%s=%s", selectable.newColumn().name(), rhs);
    }

    public static String of(Expression expression) {
        return expression.walk(new UniversalAdapter()).getOut();
    }

    public static String of(Filter filter) {
        return filter.walk(new UniversalAdapter()).getOut();
    }

    /**
     * If we ever need to provide more specificity for a type, we can create a non-universal impl.
     */
    private static class UniversalAdapter implements Filter.Visitor, Expression.Visitor {
        private String out;

        public String getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName name) {
            out = of(name);
        }

        @Override
        public void visit(RawString rawString) {
            out = of(rawString);
        }

        @Override
        public void visit(FilterMatch filterMatch) {
            out = of(filterMatch);
        }
    }
}
