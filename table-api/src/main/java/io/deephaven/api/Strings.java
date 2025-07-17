//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationDescriptions;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterBarrier;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.filter.FilterRespectsBarrier;
import io.deephaven.api.filter.FilterSerial;
import io.deephaven.api.literal.Literal;
import org.apache.commons.text.StringEscapeUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * A set of static helpers to turn strongly-typed api arguments into their {@link String} counterparts.
 */
public class Strings {

    public static String of(ColumnName columnName) {
        return columnName.name();
    }

    public static String of(ColumnName columnName, boolean inverted) {
        return (inverted ? "!" : "") + of(columnName);
    }

    public static String ofColumnNames(Collection<? extends ColumnName> columnNames) {
        return columnNames.stream().map(Strings::of).collect(Collectors.joining(",", "[", "]"));
    }

    public static String of(RawString rawString) {
        return rawString.value();
    }

    public static String of(RawString rawString, boolean invert) {
        return of(rawString, false, invert);
    }

    private static String of(RawString rawString, boolean encapsulate, boolean invert) {
        final String inner = of(rawString);
        if (invert) {
            return "!" + encapsulate(inner);
        }
        if (encapsulate) {
            return encapsulate(inner);
        }
        return inner;
    }

    public static String of(FilterComparison comparison) {
        final String lhs = ofEncapsulated(comparison.lhs());
        final String rhs = ofEncapsulated(comparison.rhs());
        return String.format("%s %s %s", lhs, comparison.operator().javaOperator(), rhs);
    }

    public static String of(FilterIn in) {
        return in.toString();
    }

    private static String of(FilterComparison comparison, boolean encapsulate) {
        final String inner = of(comparison);
        return encapsulate ? encapsulate(inner) : inner;
    }

    public static String of(FilterIn in, boolean invert) {
        final String inner = of(in);
        return (invert ? "!" : "") + inner;
    }

    public static String of(FilterIsNull isNull) {
        return String.format("isNull(%s)", of(isNull.expression()));
    }

    public static String of(FilterIsNull isNull, boolean inverted) {
        return (inverted ? "!" : "") + of(isNull);
    }

    public static String of(FilterOr filterOr) {
        return filterOr.filters().stream().map(Strings::ofEncapsulated).collect(Collectors.joining(" || "));
    }

    private static String of(FilterOr filterOr, boolean encapsulate) {
        final String inner = of(filterOr);
        return encapsulate ? encapsulate(inner) : inner;
    }

    public static String of(FilterAnd filterAnd) {
        return filterAnd.filters().stream().map(Strings::ofEncapsulated).collect(Collectors.joining(" && "));
    }

    private static String of(FilterAnd filterAnd, boolean encapsulate) {
        final String inner = of(filterAnd);
        return encapsulate ? encapsulate(inner) : inner;
    }

    public static String of(FilterPattern pattern) {
        return pattern.toString();
    }

    public static String of(FilterPattern pattern, boolean invert) {
        final String inner = of(pattern);
        return (invert ? "!" : "") + inner;
    }

    public static String ofPairs(Collection<? extends Pair> pairs) {
        return pairs.stream().map(Strings::of).collect(Collectors.joining(",", "[", "]"));
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

    public static String ofJoinMatches(Collection<? extends JoinMatch> matches) {
        return matches.stream().map(Strings::of).collect(Collectors.joining(",", "[", "]"));
    }

    public static String of(JoinAddition addition) {
        if (addition.newColumn().equals(addition.existingColumn())) {
            return of(addition.newColumn());
        }
        return String.format("%s=%s", of(addition.newColumn()), of(addition.existingColumn()));
    }

    public static String ofJoinAdditions(Collection<? extends JoinAddition> additions) {
        return additions.stream().map(Strings::of).collect(Collectors.joining(",", "[", "]"));
    }

    public static String of(final AsOfJoinMatch joinMatch) {
        return joinMatch.leftColumn().name() + joinMatch.joinRule().operatorString() + joinMatch.rightColumn().name();
    }

    public static String of(final RangeJoinMatch rangeMatch) {
        return String.format("%s%s %s %s %s %s%s",
                rangeMatch.rangeStartRule() == RangeStartRule.LESS_THAN_OR_EQUAL_ALLOW_PRECEDING ? "<- " : "",
                rangeMatch.leftStartColumn().name(),
                rangeMatch.rangeStartRule() == RangeStartRule.LESS_THAN ? "<" : "<=",
                rangeMatch.rightRangeColumn().name(),
                rangeMatch.rangeEndRule() == RangeEndRule.GREATER_THAN ? "<" : "<=",
                rangeMatch.leftEndColumn().name(),
                rangeMatch.rangeEndRule() == RangeEndRule.GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING ? " ->" : "");
    }

    public static String of(final Aggregation aggregation) {
        return ofMap(AggregationDescriptions.of(aggregation));
    }

    public static String ofAggregations(final Collection<? extends Aggregation> aggregations) {
        return ofMap(AggregationDescriptions.of(aggregations));
    }

    public static String ofMap(final Map<String, String> map) {
        final Iterator<Map.Entry<String, String>> entries = map.entrySet().iterator();
        if (!entries.hasNext()) {
            return "[]";
        }
        final BiConsumer<StringBuilder, Map.Entry<String, String>> appender =
                (sb, e) -> sb.append(e.getKey()).append(" = ").append(e.getValue());
        final StringBuilder sb = new StringBuilder();
        appender.accept(sb.append('['), entries.next());
        while (entries.hasNext()) {
            appender.accept(sb.append(", "), entries.next());
        }
        return sb.append(']').toString();
    }

    public static String of(Selectable selectable) {
        String lhs = of(selectable.newColumn());
        if (selectable.newColumn().equals(selectable.expression())) {
            return lhs;
        }
        return String.format("%s=%s", lhs, of(selectable.expression()));
    }

    public static String of(Expression expression) {
        return expression.walk(new ExpressionAdapter(false, false));
    }

    public static String of(Filter filter) {
        return of(filter, false);
    }

    public static String of(FilterNot<?> not) {
        return of(not.filter(), true);
    }

    public static String of(Filter filter, boolean invert) {
        return filter.walk(new FilterAdapter(false, invert));
    }

    public static String of(Literal value) {
        return value.walk(new LiteralAdapter(false));
    }

    public static String of(Function function) {
        return of(function, false);
    }

    public static String of(Function function, boolean invert) {
        // <name>(<exp-1>, <exp-2>, ..., <exp-N>)
        return (invert ? "!" : "") + function.name()
                + function.arguments().stream().map(Strings::of).collect(Collectors.joining(", ", "(", ")"));
    }

    public static String of(Method method) {
        return of(method, false);
    }

    public static String of(Method method, boolean invert) {
        // <object>.<name>(<exp-1>, <exp-2>, ..., <exp-N>)
        return (invert ? "!" : "") + of(method.object()) + "." + method.name()
                + method.arguments().stream().map(Strings::of).collect(Collectors.joining(", ", "(", ")"));
    }

    public static String of(FilterSerial serial) {
        return of(serial, false);
    }

    public static String of(FilterSerial serial, boolean invert) {
        // we don't have a way to represent serial in the query language; so this can't round trip
        return "invokeSerially(" + of(serial.filter(), invert) + ")";
    }

    public static String of(FilterBarrier barrier) {
        return of(barrier, false);
    }

    public static String of(FilterBarrier barrier, boolean invert) {
        // we don't have a way to represent barrier in the query language; so this can't round trip
        final String barrierId = Arrays.toString(barrier.barriers());
        return "withBarrier(" + barrierId + ", " + of(barrier.filter(), invert) + ")";
    }

    public static String of(FilterRespectsBarrier respectsBarrier) {
        return of(respectsBarrier, false);
    }

    public static String of(FilterRespectsBarrier respectsBarrier, boolean invert) {
        // we don't have a way to represent respects barrier in the query language; so this can't round trip
        final String barrierText = Arrays.stream(respectsBarrier.respectedBarriers())
                .map(Object::toString)
                .collect(Collectors.joining(",", "[", "]"));
        return "respectsBarrier(" + barrierText + ", " + of(respectsBarrier.filter(), invert) + ")";
    }

    public static String of(boolean literal) {
        return Boolean.toString(literal);
    }

    public static String of(char literal) {
        return "'" + literal + "'";
    }

    public static String of(byte literal) {
        return "(byte)" + literal;
    }

    public static String of(short literal) {
        return "(short)" + literal;
    }

    public static String of(int literal) {
        return "(int)" + literal;
    }

    public static String of(long literal) {
        return literal + "L";
    }

    public static String of(float literal) {
        return literal + "f";
    }

    public static String of(double literal) {
        return Double.toString(literal);
    }

    public static String of(String literal) {
        return '"' + StringEscapeUtils.escapeJava(literal) + '"';
    }

    private static String ofEncapsulated(Expression expression) {
        return ofEncapsulated(expression, false);
    }

    private static String ofEncapsulated(Expression expression, boolean inverted) {
        return expression.walk(new ExpressionAdapter(true, inverted));
    }

    private static String ofEncapsulated(Filter filter) {
        return filter.walk(new FilterAdapter(true, false));
    }

    private static String encapsulate(String x) {
        return "(" + x + ")";
    }

    private static class ExpressionAdapter implements Expression.Visitor<String> {

        private final boolean encapsulate;
        private final boolean invert;

        ExpressionAdapter(boolean encapsulate, boolean invert) {
            this.encapsulate = encapsulate;
            this.invert = invert;
        }

        @Override
        public String visit(Filter filter) {
            return filter.walk(new FilterAdapter(encapsulate, invert));
        }

        @Override
        public String visit(Literal literal) {
            return literal.walk(new LiteralAdapter(invert));
        }

        @Override
        public String visit(ColumnName name) {
            return of(name, invert);
        }

        @Override
        public String visit(RawString rawString) {
            return of(rawString, encapsulate, invert);
        }

        @Override
        public String visit(Function function) {
            return of(function, invert);
        }

        @Override
        public String visit(Method method) {
            return of(method, invert);
        }
    }

    private static class FilterAdapter implements Filter.Visitor<String> {

        private final boolean encapsulate;
        private final boolean invert;

        public FilterAdapter(boolean encapsulate, boolean invert) {
            this.encapsulate = encapsulate;
            this.invert = invert;
        }

        @Override
        public String visit(FilterIsNull isNull) {
            return of(isNull, invert);
        }

        @Override
        public String visit(FilterComparison comparison) {
            return invert ? of(comparison.invert(), encapsulate) : of(comparison, encapsulate);
        }

        @Override
        public String visit(FilterIn in) {
            return of(in, invert);
        }

        @Override
        public String visit(FilterNot<?> not) {
            return not.filter().walk(new FilterAdapter(encapsulate, !invert));
        }

        @Override
        public String visit(FilterOr ors) {
            return invert ? of(ors.invert(), encapsulate) : of(ors, encapsulate);
        }

        @Override
        public String visit(FilterAnd ands) {
            return invert ? of(ands.invert(), encapsulate) : of(ands, encapsulate);
        }

        @Override
        public String visit(FilterPattern pattern) {
            return of(pattern, invert);
        }

        @Override
        public String visit(FilterSerial serial) {
            return of(serial, invert);
        }

        @Override
        public String visit(FilterBarrier barrier) {
            return of(barrier, invert);
        }

        @Override
        public String visit(FilterRespectsBarrier respectsBarrier) {
            return of(respectsBarrier, invert);
        }

        @Override
        public String visit(Function function) {
            return of(function, invert);
        }

        @Override
        public String visit(Method method) {
            return of(method, invert);
        }

        @Override
        public String visit(boolean literal) {
            return of(literal ^ invert);
        }

        @Override
        public String visit(RawString rawString) {
            return of(rawString, encapsulate, invert);
        }
    }

    private static class LiteralAdapter implements Literal.Visitor<String> {

        private final boolean inverted;

        public LiteralAdapter(boolean inverted) {
            this.inverted = inverted;
        }

        @Override
        public String visit(boolean literal) {
            return of(inverted ^ literal);
        }

        @Override
        public String visit(char literal) {
            if (inverted) {
                throw new IllegalArgumentException();
            }
            return of(literal);
        }

        @Override
        public String visit(byte literal) {
            if (inverted) {
                throw new IllegalArgumentException();
            }
            return of(literal);
        }

        @Override
        public String visit(short literal) {
            if (inverted) {
                throw new IllegalArgumentException();
            }
            return of(literal);
        }

        @Override
        public String visit(int literal) {
            if (inverted) {
                throw new IllegalArgumentException();
            }
            return of(literal);
        }

        @Override
        public String visit(long literal) {
            if (inverted) {
                throw new IllegalArgumentException();
            }
            return of(literal);
        }

        @Override
        public String visit(float literal) {
            if (inverted) {
                throw new IllegalArgumentException();
            }
            return of(literal);
        }

        @Override
        public String visit(double literal) {
            if (inverted) {
                throw new IllegalArgumentException();
            }
            return of(literal);
        }

        @Override
        public String visit(String literal) {
            if (inverted) {
                throw new IllegalArgumentException();
            }
            return of(literal);
        }
    }
}
