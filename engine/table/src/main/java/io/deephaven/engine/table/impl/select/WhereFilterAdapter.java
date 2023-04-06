package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
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
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.gui.table.filters.Condition;

import java.util.Objects;

class WhereFilterAdapter implements Filter.Visitor<WhereFilter> {

    public static WhereFilter of(Filter filter) {
        return filter.walk(new WhereFilterAdapter(false));
    }

    public static WhereFilter of(FilterNot<?> not) {
        return not.filter().walk(new WhereFilterAdapter(true));
    }

    public static WhereFilter of(FilterOr ors) {
        return DisjunctiveFilter.makeDisjunctiveFilter(WhereFilter.from(ors.filters()));
    }

    public static WhereFilter of(FilterAnd ands) {
        return ConjunctiveFilter.makeConjunctiveFilter(WhereFilter.from(ands.filters()));
    }

    public static WhereFilter of(FilterComparison comparison) {
        return FilterComparisonAdapter.of(comparison);
    }

    public static WhereFilter of(FilterIsNull isNull) {
        return isNull.expression().walk(new ExpressionIsNullAdapter(false));
    }

    public static WhereFilter of(FilterIsNotNull isNotNull) {
        return isNotNull.expression().walk(new ExpressionIsNullAdapter(true));
    }

    public static WhereFilter of(ColumnName columnName, boolean inverted) {
        return inverted
                ? WhereFilterFactory.getExpression(String.format("!%s", columnName.name()))
                : WhereFilterFactory.getExpression(columnName.name());
    }

    public static WhereFilter of(Function function, boolean inverted) {
        return inverted
                ? WhereFilterFactory.getExpression(String.format("!%s", Strings.of(function)))
                : WhereFilterFactory.getExpression(Strings.of(function));
    }

    public static WhereFilter of(Method method, boolean inverted) {
        return inverted
                ? WhereFilterFactory.getExpression(String.format("!%s", Strings.of(method)))
                : WhereFilterFactory.getExpression(Strings.of(method));
    }

    public static WhereFilter of(IfThenElse ifThenElse, boolean inverted) {
        return inverted
                ? WhereFilterFactory.getExpression(String.format("!(%s)", Strings.of(ifThenElse)))
                : WhereFilterFactory.getExpression(Strings.of(ifThenElse));
    }

    public static WhereFilter of(boolean literal) {
        return literal ? WhereAllFilter.INSTANCE : WhereNoneFilter.INSTANCE;
    }

    public static WhereFilter of(RawString rawString, boolean inverted) {
        return inverted
                ? WhereFilterFactory.getExpression(String.format("!(%s)", rawString.value()))
                : WhereFilterFactory.getExpression(rawString.value());
    }

    private final boolean inverted;

    WhereFilterAdapter(boolean inverted) {
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
        return inverted ? of(isNull.invert()) : of(isNull);
    }

    @Override
    public WhereFilter visit(FilterIsNotNull isNotNull) {
        return inverted ? of(isNotNull.invert()) : of(isNotNull);
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
    public WhereFilter visit(Function function) {
        return of(function, inverted);
    }

    @Override
    public WhereFilter visit(Method method) {
        return of(method, inverted);
    }

    @Override
    public WhereFilter visit(IfThenElse ifThenElse) {
        return of(ifThenElse, inverted);
    }

    @Override
    public WhereFilter visit(boolean literal) {
        return of(inverted ^ literal);
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
                    // Note: we can't assume IntRangeFilter - even though the rhs literal is an int, it might be against
                    // a different column type; we won't have the proper typing info until execution time.
                    case LESS_THAN:
                        return new RangeConditionFilter(lhs.name(), Condition.LESS_THAN, Integer.toString(rhs));
                    case LESS_THAN_OR_EQUAL:
                        return new RangeConditionFilter(lhs.name(), Condition.LESS_THAN_OR_EQUAL,
                                Integer.toString(rhs));
                    case GREATER_THAN:
                        return new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN, Integer.toString(rhs));
                    case GREATER_THAN_OR_EQUAL:
                        return new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN_OR_EQUAL,
                                Integer.toString(rhs));
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
                    // Note: we can't assume LongRangeFilter - even though the rhs literal is an int, it might be
                    // against a different column type; we won't have the proper typing info until execution time.
                    case LESS_THAN:
                        return new RangeConditionFilter(lhs.name(), Condition.LESS_THAN, Long.toString(rhs));
                    case LESS_THAN_OR_EQUAL:
                        return new RangeConditionFilter(lhs.name(), Condition.LESS_THAN_OR_EQUAL, Long.toString(rhs));
                    case GREATER_THAN:
                        return new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN, Long.toString(rhs));
                    case GREATER_THAN_OR_EQUAL:
                        return new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN_OR_EQUAL,
                                Long.toString(rhs));
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
            public WhereFilter visit(IfThenElse ifThenElse) {
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
        public WhereFilter visit(IfThenElse lhs) {
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

        public static MatchFilter of(ColumnName columnName, boolean inverted) {
            return new MatchFilter(inverted ? MatchType.Inverted : MatchType.Regular, columnName.name(),
                    new Object[] {null});
        }

        public static WhereFilter of(Literal literal, boolean inverted) {
            // Note: we _could_ try and optimize here, since a literal is never null.
            // That said, this filter will be compiled and potentially JITted, so it might not matter.
            // return inverted ? WhereAllFilter.INSTANCE : WhereNoneFilter.INSTANCE;
            return WhereFilterFactory
                    .getExpression(String.format(inverted ? "!isNull(%s)" : "isNull(%s)", Strings.of(literal)));
        }

        public static WhereFilter of(Filter filter, boolean inverted) {
            // Note: we _could_ try and optimize here, since a filter never returns null (always true or false).
            // That said, this filter will be compiled and potentially JITted, so it might not matter.
            // return inverted ? WhereAllFilter.INSTANCE : WhereNoneFilter.INSTANCE;
            return WhereFilterFactory
                    .getExpression(String.format(inverted ? "!isNull(%s)" : "isNull(%s)", Strings.of(filter)));
        }

        public static WhereFilter of(Function function, boolean inverted) {
            return WhereFilterFactory
                    .getExpression(String.format(inverted ? "!isNull(%s)" : "isNull(%s)", Strings.of(function)));
        }

        public static WhereFilter of(Method method, boolean inverted) {
            return WhereFilterFactory
                    .getExpression(String.format(inverted ? "!isNull(%s)" : "isNull(%s)", Strings.of(method)));
        }

        public static WhereFilter of(IfThenElse ifThenElse, boolean inverted) {
            return WhereFilterFactory
                    .getExpression(String.format(inverted ? "!isNull(%s)" : "isNull(%s)", Strings.of(ifThenElse)));
        }

        public static WhereFilter of(RawString rawString, boolean inverted) {
            return WhereFilterFactory
                    .getExpression(String.format(inverted ? "!isNull(%s)" : "isNull(%s)", Strings.of(rawString)));
        }

        private final boolean inverted;

        ExpressionIsNullAdapter(boolean inverted) {
            this.inverted = inverted;
        }

        @Override
        public WhereFilter visit(Literal literal) {
            return of(literal, inverted);
        }

        @Override
        public WhereFilter visit(ColumnName columnName) {
            return of(columnName, inverted);
        }

        @Override
        public WhereFilter visit(Filter filter) {
            return of(filter, inverted);
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
        public WhereFilter visit(IfThenElse ifThenElse) {
            return of(ifThenElse, inverted);
        }

        @Override
        public WhereFilter visit(RawString rawString) {
            return of(rawString, inverted);
        }
    }
}
