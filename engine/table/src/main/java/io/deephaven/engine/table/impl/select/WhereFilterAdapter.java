//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.literal.Literal;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.gui.table.filters.Condition;

import java.util.Objects;
import java.util.stream.Collectors;

class WhereFilterAdapter implements Filter.Visitor<WhereFilter> {

    public static WhereFilter of(Filter filter) {
        return of(filter, false);
    }

    public static WhereFilter of(FilterNot<?> not) {
        return of(not, false);
    }

    public static WhereFilter of(FilterOr ors) {
        // A || B || ... || Z
        return DisjunctiveFilter.of(ors);
    }

    public static WhereFilter of(FilterAnd ands) {
        // A && B && ... && Z
        return ConjunctiveFilter.of(ands);
    }

    public static WhereFilter of(FilterComparison comparison) {
        return FilterComparisonAdapter.of(comparison);
    }

    public static WhereFilter of(FilterIn in) {
        return of(in, false);
    }

    public static WhereFilter of(FilterIsNull isNull) {
        return of(isNull, false);
    }

    public static WhereFilter of(FilterPattern pattern) {
        return WhereFilterPatternImpl.of(pattern);
    }

    public static WhereFilter of(Function function) {
        return of(function, false);
    }

    public static WhereFilter of(Method method) {
        return of(method, false);
    }

    public static WhereFilter of(RawString rawString) {
        return of(rawString, false);
    }

    public static WhereFilter of(boolean literal) {
        throw new UnsupportedOperationException("Should not be constructing literal boolean WhereFilter");
    }

    public static WhereFilter of(Filter filter, boolean inverted) {
        return filter.walk(new WhereFilterAdapter(inverted));
    }

    public static WhereFilter of(FilterNot<?> not, boolean inverted) {
        return not.filter().walk(new WhereFilterAdapter(!inverted));
    }

    public static WhereFilter of(FilterOr ors, boolean inverted) {
        // !A && !B && ... && !Z (inverted)
        // A || B || ... || Z (regular)
        return inverted ? of(ors.invert()) : of(ors);
    }

    public static WhereFilter of(FilterAnd ands, boolean inverted) {
        // !A || !B || ... || !Z (inverted)
        // A && B && ... && Z (regular)
        return inverted ? of(ands.invert()) : of(ands);
    }

    public static WhereFilter of(FilterComparison comparison, boolean inverted) {
        return of(inverted ? comparison.invert() : comparison);
    }

    public static WhereFilter of(FilterIn in, boolean inverted) {
        if (in.values().size() == 1) {
            // Simplified case, handles transpositions of LHS / RHS most optimally
            return of(in.asEquals(), inverted);
        }
        if (in.expression() instanceof ColumnName) {
            // In the case where LHS is a column name, we want to be as efficient as possible and only read that column
            // data once. MatchFilter allows us to do that.
            if (in.values().stream().allMatch(p -> p instanceof Literal)) {
                return MatchFilter.ofLiterals(
                        ((ColumnName) in.expression()).name(),
                        in.values().stream().map(Literal.class::cast).collect(Collectors.toList()),
                        inverted);
            }
            // It would be nice if there was a way to allow efficient read access with Disjunctive / Conjunctive
            // constructions. Otherwise, we fall back to Condition filter.
            // TODO(deephaven-core#3791): Non-vectorized version of Disjunctive / Conjunctive filters
            // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
            return WhereFilterFactory.getExpression(Strings.of(in.asComparisons(), inverted));
        }
        return of(in.asComparisons(), inverted);
    }

    public static WhereFilter of(FilterIsNull isNull, boolean inverted) {
        return isNull.expression().walk(new ExpressionIsNullAdapter(inverted));
    }

    public static WhereFilter of(FilterPattern pattern, boolean inverted) {
        final WhereFilter filter = of(pattern);
        return inverted ? WhereFilterInvertedImpl.of(filter) : filter;
    }

    public static WhereFilter of(Function function, boolean inverted) {
        // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
        return WhereFilterFactory.getExpression(Strings.of(function, inverted));
    }

    public static WhereFilter of(Method method, boolean inverted) {
        // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
        return WhereFilterFactory.getExpression(Strings.of(method, inverted));
    }

    public static WhereFilter of(RawString rawString, boolean inverted) {
        // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
        return WhereFilterFactory.getExpression(Strings.of(rawString, inverted));
    }

    private final boolean inverted;

    private WhereFilterAdapter(boolean inverted) {
        this.inverted = inverted;
    }

    @Override
    public WhereFilter visit(FilterNot<?> not) {
        return of(not, inverted);
    }

    @Override
    public WhereFilter visit(FilterComparison comparison) {
        return of(comparison, inverted);
    }

    @Override
    public WhereFilter visit(FilterIn in) {
        return of(in, inverted);
    }

    @Override
    public WhereFilter visit(FilterIsNull isNull) {
        return of(isNull, inverted);
    }

    @Override
    public WhereFilter visit(FilterOr ors) {
        return of(ors, inverted);
    }

    @Override
    public WhereFilter visit(FilterAnd ands) {
        return of(ands, inverted);
    }

    @Override
    public WhereFilter visit(FilterPattern pattern) {
        return of(pattern, inverted);
    }

    @Override
    public WhereFilter visit(Function function) {
        return of(function, inverted);
    }

    @Override
    public WhereFilter visit(Method method) {
        return of(method, inverted);
    }

    @Override
    public WhereFilter visit(boolean literal) {
        return of(literal ^ inverted);
    }

    @Override
    public WhereFilter visit(RawString rawString) {
        return of(rawString, inverted);
    }

    private static class FilterComparisonAdapter implements Expression.Visitor<WhereFilter> {

        public static WhereFilter of(FilterComparison condition) {
            FilterComparison preferred = condition.maybeTranspose();
            return preferred.lhs().walk(new FilterComparisonAdapter(condition, preferred));
        }

        private final FilterComparison original;
        private final FilterComparison preferred;

        private FilterComparisonAdapter(FilterComparison original, FilterComparison preferred) {
            this.original = Objects.requireNonNull(original);
            this.preferred = Objects.requireNonNull(preferred);
        }

        private WhereFilter original() {
            // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
            return WhereFilterFactory.getExpression(Strings.of(original));
        }

        @Override
        public WhereFilter visit(ColumnName lhs) {
            return preferred.rhs().walk(new FilterComparisonAdapter.PreferredLhsColumnRhsVisitor(lhs));
        }

        private class PreferredLhsColumnRhsVisitor
                implements Expression.Visitor<WhereFilter>, Literal.Visitor<WhereFilter> {
            private final ColumnName lhs;

            public PreferredLhsColumnRhsVisitor(ColumnName lhs) {
                this.lhs = Objects.requireNonNull(lhs);
            }

            // The String vs non-String cases are separated out, as it's necessary in the RangeFilter case to
            // wrap String literals with quotes (as that's what RangeFilter expects wrt parsing). MatchFilter
            // allows us to pass in the already parsed Object (otherwise, if we were passing strValues we would need to
            // wrap them)

            private WhereFilter matchOrRange(Object rhsLiteral) {
                // TODO(deephaven-core#3730): More efficient io.deephaven.api.filter.FilterComparison to RangeFilter
                switch (preferred.operator()) {
                    case EQUALS:
                        return new MatchFilter(MatchType.Regular, lhs.name(), rhsLiteral);
                    case NOT_EQUALS:
                        return new MatchFilter(MatchType.Inverted, lhs.name(), rhsLiteral);
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        return range(rhsLiteral);
                    default:
                        throw new IllegalStateException("Unexpected operator " + original.operator());
                }
            }

            private WhereFilter matchOrRange(String rhsLiteral) {
                // TODO(deephaven-core#3730): More efficient io.deephaven.api.filter.FilterComparison to RangeFilter
                switch (preferred.operator()) {
                    case EQUALS:
                        return new MatchFilter(MatchType.Regular, lhs.name(), rhsLiteral);
                    case NOT_EQUALS:
                        return new MatchFilter(MatchType.Inverted, lhs.name(), rhsLiteral);
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        return range(rhsLiteral);
                    default:
                        throw new IllegalStateException("Unexpected operator " + original.operator());
                }
            }

            @Override
            public WhereFilter visit(ColumnName rhs) {
                // LHS column = RHS column
                return original();
            }

            @Override
            public WhereFilter visit(int rhs) {
                return matchOrRange(rhs);
            }

            @Override
            public WhereFilter visit(long rhs) {
                return matchOrRange(rhs);
            }

            @Override
            public WhereFilter visit(byte rhs) {
                return matchOrRange(rhs);
            }

            @Override
            public WhereFilter visit(short rhs) {
                return matchOrRange(rhs);
            }

            @Override
            public WhereFilter visit(float rhs) {
                return matchOrRange(rhs);
            }

            @Override
            public WhereFilter visit(double rhs) {
                return matchOrRange(rhs);
            }

            @Override
            public WhereFilter visit(char rhs) {
                return matchOrRange(rhs);
            }

            @Override
            public WhereFilter visit(String rhs) {
                return matchOrRange(rhs);
            }

            @Override
            public WhereFilter visit(boolean rhs) {
                switch (preferred.operator()) {
                    case EQUALS:
                        return new MatchFilter(MatchType.Regular, lhs.name(), rhs);
                    case NOT_EQUALS:
                        return new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        return original();
                    default:
                        throw new IllegalStateException("Unexpected operator " + original.operator());
                }
            }

            @Override
            public WhereFilter visit(Filter rhs) {
                return original();
            }

            @Override
            public WhereFilter visit(Function function) {
                return original();
            }

            @Override
            public WhereFilter visit(Method method) {
                return original();
            }

            @Override
            public WhereFilter visit(Literal value) {
                return value.walk((Literal.Visitor<WhereFilter>) this);
            }

            @Override
            public WhereFilter visit(RawString rawString) {
                return original();
            }

            private RangeFilter range(Object rhsLiteral) {
                // TODO(deephaven-core#3730): More efficient io.deephaven.api.filter.FilterComparison to RangeFilter
                final String rhsLiteralAsStr = rhsLiteral.toString();
                switch (preferred.operator()) {
                    case LESS_THAN:
                        return new RangeFilter(lhs.name(), Condition.LESS_THAN, rhsLiteralAsStr);
                    case LESS_THAN_OR_EQUAL:
                        return new RangeFilter(lhs.name(), Condition.LESS_THAN_OR_EQUAL, rhsLiteralAsStr);
                    case GREATER_THAN:
                        return new RangeFilter(lhs.name(), Condition.GREATER_THAN, rhsLiteralAsStr);
                    case GREATER_THAN_OR_EQUAL:
                        return new RangeFilter(lhs.name(), Condition.GREATER_THAN_OR_EQUAL, rhsLiteralAsStr);
                }
                throw new IllegalStateException("Unexpected");
            }

            private RangeFilter range(String rhsLiteral) {
                // TODO(deephaven-core#3730): More efficient io.deephaven.api.filter.FilterComparison to RangeFilter
                final String quotedRhsLiteral = '"' + rhsLiteral + '"';
                switch (preferred.operator()) {
                    case LESS_THAN:
                        return new RangeFilter(lhs.name(), Condition.LESS_THAN, quotedRhsLiteral);
                    case LESS_THAN_OR_EQUAL:
                        return new RangeFilter(lhs.name(), Condition.LESS_THAN_OR_EQUAL, quotedRhsLiteral);
                    case GREATER_THAN:
                        return new RangeFilter(lhs.name(), Condition.GREATER_THAN, quotedRhsLiteral);
                    case GREATER_THAN_OR_EQUAL:
                        return new RangeFilter(lhs.name(), Condition.GREATER_THAN_OR_EQUAL, quotedRhsLiteral);
                }
                throw new IllegalStateException("Unexpected");
            }
        }

        // Note for all remaining cases: since we are walking the preferred object, we know we don't have to handle
        // the case where rhs is column name.

        @Override
        public WhereFilter visit(Literal lhs) {
            return original();
        }

        @Override
        public WhereFilter visit(Filter lhs) {
            return original();
        }

        @Override
        public WhereFilter visit(Function lhs) {
            return original();
        }

        @Override
        public WhereFilter visit(Method lhs) {
            return original();
        }

        @Override
        public WhereFilter visit(RawString lhs) {
            return original();
        }
    }

    private static class ExpressionIsNullAdapter implements Expression.Visitor<WhereFilter> {

        public static WhereFilter of(Expression expression) {
            return expression.walk(new ExpressionIsNullAdapter(false));
        }

        private final boolean inverted;

        ExpressionIsNullAdapter(boolean inverted) {
            this.inverted = inverted;
        }

        private WhereFilter getExpression(String x) {
            // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
            return WhereFilterFactory.getExpression((inverted ? "!isNull(" : "isNull(") + x + ")");
        }

        @Override
        public WhereFilter visit(ColumnName columnName) {
            return new MatchFilter(
                    inverted ? MatchType.Inverted : MatchType.Regular,
                    columnName.name(),
                    new Object[] {null});
        }

        // Note: it might be tempting to consolidate all of the following getExpression calls to a common function, but
        // then we'd be losing the type information that allows us to call the more explicitly typed Strings#of(<type>)
        // methods.

        @Override
        public WhereFilter visit(Literal literal) {
            // Note: we _could_ try and optimize here, since a literal is never null.
            // That said, this filter will be compiled and potentially JITted, so it might not matter.
            // return inverted ? WhereAllFilter.INSTANCE : WhereNoneFilter.INSTANCE;
            return getExpression(Strings.of(literal));
        }

        @Override
        public WhereFilter visit(Filter filter) {
            // Note: we _could_ try and optimize here, since a filter never returns null (always true or false).
            // That said, this filter will be compiled and potentially JITted, so it might not matter.
            // return inverted ? WhereAllFilter.INSTANCE : WhereNoneFilter.INSTANCE;
            return getExpression(Strings.of(filter));
        }

        @Override
        public WhereFilter visit(Function function) {
            return getExpression(Strings.of(function));
        }

        @Override
        public WhereFilter visit(Method method) {
            return getExpression(Strings.of(method));
        }

        @Override
        public WhereFilter visit(RawString rawString) {
            return getExpression(Strings.of(rawString));
        }
    }
}
