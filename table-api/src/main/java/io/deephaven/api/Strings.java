/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.expression.BinaryExpression;
import io.deephaven.api.expression.BinaryFunction;
import io.deephaven.api.expression.Divide;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Minus;
import io.deephaven.api.expression.Multiply;
import io.deephaven.api.expression.Plus;
import io.deephaven.api.expression.UnaryExpression;
import io.deephaven.api.expression.UnaryMinus;
import io.deephaven.api.expression.UnaryFunction;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIsNotNull;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.value.Literal;

import java.util.Collection;
import java.util.Objects;
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
        // todo: consider lhs / rhs as null? translate to isNull / !isNull?
        String lhs = of(comparison.lhs());
        String rhs = of(comparison.rhs());
        switch (comparison.operator()) {
            case LESS_THAN:
                return String.format("(%s) < (%s)", lhs, rhs);
            case LESS_THAN_OR_EQUAL:
                return String.format("(%s) <= (%s)", lhs, rhs);
            case GREATER_THAN:
                return String.format("(%s) > (%s)", lhs, rhs);
            case GREATER_THAN_OR_EQUAL:
                return String.format("(%s) >= (%s)", lhs, rhs);
            case EQUALS:
                return String.format("(%s) == (%s)", lhs, rhs);
            case NOT_EQUALS:
                return String.format("(%s) != (%s)", lhs, rhs);
            default:
                throw new IllegalStateException(
                        "Unexpected comparison operator: " + comparison.operator());
        }
    }

    public static String of(FilterNot not) {
        return String.format("!(%s)", of(not.filter()));
    }

    public static String of(FilterIsNull isNull) {
        return String.format("isNull(%s)", of(isNull.expression()));
    }

    public static String of(FilterIsNotNull isNotNull) {
        return String.format("!isNull(%s)", of(isNotNull.expression()));
    }

    public static String of(FilterOr filterOr) {
        return filterOr.filters().stream().map(Strings::of)
                .collect(Collectors.joining(") || (", "(", ")"));
    }

    public static String of(FilterAnd filterAnd) {
        return filterAnd.filters().stream().map(Strings::of)
                .collect(Collectors.joining(") && (", "(", ")"));
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
        String rhs = selectable.expression().walk(new UniversalAdapter()).getOut();
        return String.format("%s=%s", lhs, rhs);
    }

    public static String of(Expression expression) {
        final UniversalAdapter visitor = new UniversalAdapter();
        expression.walk((Expression.Visitor) visitor);
        return visitor.getOut();
    }

    public static String of(BinaryExpression binaryExpression) {
        final UniversalAdapter visitor = new UniversalAdapter();
        binaryExpression.walk((BinaryExpression.Visitor) visitor);
        return visitor.getOut();
    }

    public static String of(UnaryExpression unaryExpression) {
        final UniversalAdapter visitor = new UniversalAdapter();
        unaryExpression.walk((UnaryExpression.Visitor) visitor);
        return visitor.getOut();
    }

    public static String of(Filter filter) {
        final UniversalAdapter visitor = new UniversalAdapter();
        filter.walk((Filter.Visitor) visitor);
        return visitor.getOut();
    }

    public static String of(Literal value) {
        final UniversalAdapter visitor = new UniversalAdapter();
        value.walk((Literal.Visitor) visitor);
        return visitor.getOut();
    }

    public static String of(UnaryMinus unaryMinus) {
        return String.format("-(%s)", of(unaryMinus.parent()));
    }

    public static String of(UnaryFunction unaryFunction) {
        return String.format("%s(%s)", unaryFunction.name(), of(unaryFunction.parent()));
    }

    public static String of(Plus plus) {
        return String.format("(%s) + (%s)", of(plus.lhs()), of(plus.rhs()));
    }

    public static String of(Minus minus) {
        return String.format("(%s) - (%s)", of(minus.lhs()), of(minus.rhs()));
    }

    public static String of(Multiply multiply) {
        return String.format("(%s) * (%s)", of(multiply.lhs()), of(multiply.rhs()));
    }

    public static String of(Divide divide) {
        return String.format("(%s) / (%s)", of(divide.lhs()), of(divide.rhs()));
    }

    public static String of(BinaryFunction binaryFunction) {
        return String.format("%s(%s, %s)", binaryFunction.name(), of(binaryFunction.lhs()), of(binaryFunction.rhs()));
    }

    /**
     * If we ever need to provide more specificity for a type, we can create a non-universal impl.
     */
    private static class UniversalAdapter
            implements Filter.Visitor, Expression.Visitor, Literal.Visitor, UnaryExpression.Visitor,
            BinaryExpression.Visitor {
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
        public void visit(Filter filter) {
            filter.walk((Filter.Visitor) this);
        }

        @Override
        public void visit(UnaryExpression unaryExpression) {
            unaryExpression.walk((UnaryExpression.Visitor) this);
        }

        @Override
        public void visit(BinaryExpression binaryExpression) {
            binaryExpression.walk((BinaryExpression.Visitor) this);
        }

        @Override
        public void visit(FilterComparison comparison) {
            out = of(comparison);
        }

        @Override
        public void visit(FilterIsNull isNull) {
            out = of(isNull);
        }

        @Override
        public void visit(FilterIsNotNull isNotNull) {
            out = of(isNotNull);
        }

        @Override
        public void visit(FilterNot not) {
            out = of(not);
        }

        @Override
        public void visit(FilterOr ors) {
            out = of(ors);
        }

        @Override
        public void visit(FilterAnd ands) {
            out = of(ands);
        }

        @Override
        public void visit(UnaryMinus unaryMinus) {
            out = of(unaryMinus);
        }

        @Override
        public void visit(UnaryFunction unaryFunction) {
            out = of(unaryFunction);
        }

        @Override
        public void visit(Plus plus) {
            out = of(plus);
        }

        @Override
        public void visit(Minus minus) {
            out = of(minus);
        }

        @Override
        public void visit(Multiply multiply) {
            out = of(multiply);
        }

        @Override
        public void visit(Divide divide) {
            out = of(divide);
        }

        @Override
        public void visit(BinaryFunction binaryFunction) {
            out = of(binaryFunction);
        }

        @Override
        public void visit(Literal value) {
            value.walk((Literal.Visitor) this);
        }

        @Override
        public void visit(int literal) {
            out = String.format("(int)%s", literal);
        }

        @Override
        public void visit(long literal) {
            out = String.format("%sL", literal);
        }

        @Override
        public void visit(boolean literal) {
            out = Boolean.toString(literal);
        }
    }
}
