/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.IfThenElse;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIsNotNull;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.literal.Literal;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * A set of static helpers to turn strongly-typed api arguments into their {@link String} counterparts.
 */
public class Strings {

    public static String of(ColumnName columnName) {
        return columnName.name();
    }

    public static String of(RawString rawString) {
        return rawString.value();
    }

    public static String of(FilterComparison comparison) {
        final String lhs = ofEncapsulated(comparison.lhs());
        final String rhs = ofEncapsulated(comparison.rhs());
        return String.format("%s %s %s", lhs, comparison.operator().javaOperator(), rhs);
    }

    public static String of(FilterNot<?> not) {
        return String.format("!%s", ofEncapsulated(not.filter()));
    }

    public static String of(FilterIsNull isNull) {
        return String.format("isNull(%s)", of(isNull.expression()));
    }

    public static String of(FilterIsNotNull isNotNull) {
        return String.format("!isNull(%s)", of(isNotNull.expression()));
    }

    public static String of(FilterOr filterOr) {
        return filterOr.filters().stream().map(Strings::ofEncapsulated).collect(Collectors.joining(" || "));
    }

    public static String of(FilterAnd filterAnd) {
        return filterAnd.filters().stream().map(Strings::ofEncapsulated).collect(Collectors.joining(" && "));
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

    public static String of(Collection<? extends JoinAddition> additions) {
        return additions.stream().map(Strings::of).collect(Collectors.joining(",", "[", "]"));
    }

    public static String of(Selectable selectable) {
        String lhs = of(selectable.newColumn());
        if (selectable.newColumn().equals(selectable.expression())) {
            return lhs;
        }
        return String.format("%s=%s", lhs, of(selectable.expression()));
    }

    public static String of(Expression expression) {
        return expression.walk(new UniversalAdapter(false));
    }

    public static String of(Filter filter) {
        return filter.walk((Filter.Visitor<String>) new UniversalAdapter(false));
    }

    public static String of(Literal value) {
        return value.walk((Literal.Visitor<String>) new UniversalAdapter(false));
    }

    public static String of(Function function) {
        // <name>(<exp-1>, <exp-2>, ..., <exp-N>)
        return function.name()
                + function.arguments().stream().map(Strings::of).collect(Collectors.joining(", ", "(", ")"));
    }

    public static String of(Method method) {
        // <object>.<name>(<exp-1>, <exp-2>, ..., <exp-N>)
        return of(method.object()) + "." + method.name()
                + method.arguments().stream().map(Strings::of).collect(Collectors.joining(", ", "(", ")"));
    }

    public static String of(IfThenElse ifThenElse) {
        // <condition> ? <if-true> : <if-false>
        return String.format("%s ? %s : %s",
                ofEncapsulated(ifThenElse.condition()),
                ofEncapsulated(ifThenElse.ifTrue()),
                ofEncapsulated(ifThenElse.ifFalse()));
    }

    public static String of(boolean literal) {
        return Boolean.toString(literal);
    }

    private static String ofEncapsulated(Expression expression) {
        return expression.walk(new UniversalAdapter(true));
    }

    private static String ofEncapsulated(Filter filter) {
        return filter.walk((Filter.Visitor<String>) new UniversalAdapter(true));
    }

    /**
     * If we ever need to provide more specificity for a type, we can create a non-universal impl.
     */
    private static class UniversalAdapter
            implements Expression.Visitor<String>, Filter.Visitor<String>, Literal.Visitor<String> {

        private final boolean encapsulate;

        UniversalAdapter(boolean encapsulate) {
            this.encapsulate = encapsulate;
        }

        private String encapsulate(String x) {
            return encapsulate ? "(" + x + ")" : x;
        }

        @Override
        public String visit(Filter filter) {
            return filter.walk((Filter.Visitor<String>) this);
        }

        @Override
        public String visit(Literal literal) {
            return literal.walk((Literal.Visitor<String>) this);
        }

        @Override
        public String visit(ColumnName name) {
            return of(name);
        }

        @Override
        public String visit(RawString rawString) {
            return encapsulate(of(rawString));
        }

        @Override
        public String visit(Function function) {
            return of(function);
        }

        @Override
        public String visit(Method method) {
            return of(method);
        }

        @Override
        public String visit(IfThenElse ifThenElse) {
            return encapsulate(of(ifThenElse));
        }

        @Override
        public String visit(FilterComparison comparison) {
            return encapsulate(of(comparison));
        }

        @Override
        public String visit(FilterIsNull isNull) {
            return of(isNull);
        }

        @Override
        public String visit(FilterIsNotNull isNotNull) {
            return of(isNotNull);
        }

        @Override
        public String visit(FilterNot<?> not) {
            return of(not);
        }

        @Override
        public String visit(FilterOr ors) {
            return encapsulate(of(ors));
        }

        @Override
        public String visit(FilterAnd ands) {
            return encapsulate(of(ands));
        }

        @Override
        public String visit(int literal) {
            return String.format("(int)%s", literal);
        }

        @Override
        public String visit(long literal) {
            return String.format("%sL", literal);
        }

        @Override
        public String visit(boolean literal) {
            return of(literal);
        }
    }
}
