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
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterMatches;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.filter.FilterQuick;
import io.deephaven.api.literal.Literal;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.gui.table.filters.Condition;

import java.util.Objects;

public class WhereFilterAdapter implements Filter.Visitor<WhereFilter> {

    public static WhereFilter of(Filter filter) {
        return filter.walk(new WhereFilterAdapter(false));
    }

    public static WhereFilter of(FilterNot<?> not) {
        return not.filter().walk(new WhereFilterAdapter(true));
    }

    public static WhereFilter of(FilterOr ors) {
        return DisjunctiveFilter.of(ors);
    }

    public static WhereFilter of(FilterAnd ands) {
        return ConjunctiveFilter.of(ands);
    }

    public static WhereFilter of(FilterComparison comparison) {
        return FilterComparisonAdapter.of(comparison);
    }

    public static WhereFilter of(FilterIsNull isNull, boolean inverted) {
        return isNull.expression().walk(new ExpressionIsNullAdapter(inverted));
    }

    public static WhereFilter of(FilterPattern pattern, boolean inverted) {
        return WhereFilterPatternImpl.of(pattern, inverted);
    }

    public static WhereFilter of(FilterQuick quick, boolean inverted) {
        return WhereFilterQuickImpl.of(quick, inverted);
    }

    public static WhereFilter of(FilterMatches matches, boolean inverted) {
        return MatchFilter.ofStringValues(matches, inverted);
    }

    public static WhereFilter of(ColumnName columnName, boolean inverted) {
        final String x = String.format(inverted ? "isNull(%s) || !%s" : "!isNull(%s) && %s", columnName.name(),
                columnName.name());
        return WhereFilterFactory.getExpression(x);
    }

    public static WhereFilter of(Function function, boolean inverted) {
        // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
        return WhereFilterFactory.getExpression(Strings.of(function, inverted));
    }

    public static WhereFilter of(Method method, boolean inverted) {
        // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
        return WhereFilterFactory.getExpression(Strings.of(method, inverted));
    }

    public static WhereFilter of(boolean literal) {
        throw new UnsupportedOperationException("Should not be constructing literal boolean WhereFilter");
    }

    public static WhereFilter of(RawString rawString, boolean inverted) {
        // TODO(deephaven-core#3740): Remove engine crutch on io.deephaven.api.Strings
        return WhereFilterFactory.getExpression(Strings.of(rawString, false, inverted));
    }

    private final boolean inverted;

    private WhereFilterAdapter(boolean inverted) {
        this.inverted = inverted;
    }

    @Override
    public WhereFilter visit(FilterComparison comparison) {
        return of(inverted ? comparison.invert() : comparison);
    }

    @Override
    public WhereFilter visit(FilterNot<?> not) {
        return inverted ? of(not.invert()) : of(not);
    }

    @Override
    public WhereFilter visit(FilterIsNull isNull) {
        return of(isNull, inverted);
    }

    @Override
    public WhereFilter visit(FilterOr ors) {
        // !A && !B && ... && !Z
        // A || B || ... || Z
        return inverted ? of(ors.invert()) : of(ors);
    }

    @Override
    public WhereFilter visit(FilterAnd ands) {
        // !A || !B || ... || !Z
        // A && B && ... && Z
        return inverted ? of(ands.invert()) : of(ands);
    }

    @Override
    public WhereFilter visit(ColumnName columnName) {
        return of(columnName, inverted);
    }

    @Override
    public WhereFilter visit(FilterPattern pattern) {
        return of(pattern, inverted);
    }

    @Override
    public WhereFilter visit(FilterQuick quick) {
        return of(quick, inverted);
    }

    @Override
    public WhereFilter visit(FilterMatches matches) {
        return of(matches, inverted);
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

            @Override
            public WhereFilter visit(ColumnName rhs) {
                // LHS column = RHS column
                return original();
            }

            @Override
            public WhereFilter visit(int rhs) {
                switch (preferred.operator()) {
                    case EQUALS:
                        return new MatchFilter(lhs.name(), rhs);
                    case NOT_EQUALS:
                        return new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        return range(Integer.toString(rhs));
                    default:
                        throw new IllegalStateException("Unexpected operator " + original.operator());
                }
            }

            @Override
            public WhereFilter visit(long rhs) {
                switch (preferred.operator()) {
                    case EQUALS:
                        return new MatchFilter(lhs.name(), rhs);
                    case NOT_EQUALS:
                        return new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        return range(Long.toString(rhs));
                    default:
                        throw new IllegalStateException("Unexpected operator " + original.operator());
                }
            }

            @Override
            public WhereFilter visit(boolean rhs) {
                switch (preferred.operator()) {
                    case EQUALS:
                        return new MatchFilter(lhs.name(), rhs);
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
            public WhereFilter visit(String rhs) {
                switch (preferred.operator()) {
                    case EQUALS:
                        return new MatchFilter(lhs.name(), rhs);
                    case NOT_EQUALS:
                        return new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        return range(rhs);
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

            private RangeConditionFilter range(String rhsValue) {
                // TODO(deephaven-core#3730): More efficient io.deephaven.api.filter.FilterComparison to RangeFilter
                switch (preferred.operator()) {
                    case LESS_THAN:
                        return new RangeConditionFilter(lhs.name(), Condition.LESS_THAN, rhsValue);
                    case LESS_THAN_OR_EQUAL:
                        return new RangeConditionFilter(lhs.name(), Condition.LESS_THAN_OR_EQUAL, rhsValue);
                    case GREATER_THAN:
                        return new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN, rhsValue);
                    case GREATER_THAN_OR_EQUAL:
                        return new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN_OR_EQUAL, rhsValue);
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
