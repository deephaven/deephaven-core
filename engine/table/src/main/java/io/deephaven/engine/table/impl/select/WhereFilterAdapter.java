package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.ExpressionFunction;
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

class WhereFilterAdapter implements Filter.Visitor {

    public static WhereFilter of(Filter filter) {
        return filter.walk(new WhereFilterAdapter(false)).out();
    }

    public static WhereFilter of(FilterNot<?> not) {
        return not.filter().walk(new WhereFilterAdapter(true)).out();
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
        return isNull.expression().walk(new ExpressionIsNullAdapter(false)).out();
    }

    public static WhereFilter of(FilterIsNotNull isNotNull) {
        return isNotNull.expression().walk(new ExpressionIsNullAdapter(true)).out();
    }

    public static WhereFilter of(boolean literal) {
        return literal ? WhereAllFilter.INSTANCE : WhereNoneFilter.INSTANCE;
    }

    public static WhereFilter of(RawString rawString, boolean inverted) {
        return inverted ? WhereFilterFactory.getExpression(String.format("!(%s)", rawString.value()))
                : WhereFilterFactory.getExpression(rawString.value());
    }

    private final boolean inverted;
    private WhereFilter out;

    WhereFilterAdapter(boolean inverted) {
        this.inverted = inverted;
    }

    public WhereFilter out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(FilterComparison comparison) {
        out = of(inverted ? comparison.inverse() : comparison);
    }

    @Override
    public void visit(FilterNot<?> not) {
        out = inverted ? of(not.inverse()) : of(not);
    }

    @Override
    public void visit(FilterIsNull isNull) {
        out = inverted ? of(isNull.inverse()) : of(isNull);
    }

    @Override
    public void visit(FilterIsNotNull isNotNull) {
        out = inverted ? of(isNotNull.inverse()) : of(isNotNull);
    }

    @Override
    public void visit(FilterOr ors) {
        // !A && !B && ... && !Z
        // A || B || ... || Z
        out = inverted ? of(ors.inverse()) : of(ors);
    }

    @Override
    public void visit(FilterAnd ands) {
        // !A || !B || ... || !Z
        // A && B && ... && Z
        out = inverted ? of(ands.inverse()) : of(ands);
    }

    @Override
    public void visit(boolean literal) {
        out = of(inverted ^ literal);
    }

    @Override
    public void visit(RawString rawString) {
        out = of(rawString, inverted);
    }

    private static class FilterComparisonAdapter implements Expression.Visitor {

        public static WhereFilter of(FilterComparison condition) {
            FilterComparison preferred = condition.maybeTranspose();
            return preferred.lhs().walk(new FilterComparisonAdapter(condition, preferred)).getOut();
        }

        private final FilterComparison original;
        private final FilterComparison preferred;

        private WhereFilter out;

        private FilterComparisonAdapter(FilterComparison original, FilterComparison preferred) {
            this.original = Objects.requireNonNull(original);
            this.preferred = Objects.requireNonNull(preferred);
        }

        public WhereFilter getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName lhs) {
            preferred.rhs().walk(new FilterComparisonAdapter.PreferredLhsColumnRhsVisitor(lhs));
        }

        private class PreferredLhsColumnRhsVisitor implements Expression.Visitor, Literal.Visitor {
            private final ColumnName lhs;

            public PreferredLhsColumnRhsVisitor(ColumnName lhs) {
                this.lhs = Objects.requireNonNull(lhs);
            }

            @Override
            public void visit(ColumnName rhs) {
                // LHS column = RHS column
                out = WhereFilterFactory.getExpression(Strings.of(original));
            }

            @Override
            public void visit(int rhs) {
                switch (preferred.operator()) {
                    case EQUALS:
                        out = new MatchFilter(lhs.name(), rhs);
                        break;
                    case NOT_EQUALS:
                        out = new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                        break;
                    // Note: we can't assume IntRangeFilter - even though the rhs literal is an int, it might be against
                    // a different column type; we won't have the proper typing info until execution time.
                    case LESS_THAN:
                        out = new RangeConditionFilter(lhs.name(), Condition.LESS_THAN, Integer.toString(rhs));
                        break;
                    case LESS_THAN_OR_EQUAL:
                        out = new RangeConditionFilter(lhs.name(), Condition.LESS_THAN_OR_EQUAL, Integer.toString(rhs));
                        break;
                    case GREATER_THAN:
                        out = new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN, Integer.toString(rhs));
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        out = new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN_OR_EQUAL,
                                Integer.toString(rhs));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected operator " + original.operator());
                }
            }

            @Override
            public void visit(long rhs) {
                switch (preferred.operator()) {
                    case EQUALS:
                        out = new MatchFilter(lhs.name(), rhs);
                        break;
                    case NOT_EQUALS:
                        out = new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                        break;
                    // Note: we can't assume LongRangeFilter - even though the rhs literal is an int, it might be
                    // against a different column type; we won't have the proper typing info until execution time.
                    case LESS_THAN:
                        out = new RangeConditionFilter(lhs.name(), Condition.LESS_THAN, Long.toString(rhs));
                        break;
                    case LESS_THAN_OR_EQUAL:
                        out = new RangeConditionFilter(lhs.name(), Condition.LESS_THAN_OR_EQUAL, Long.toString(rhs));
                        break;
                    case GREATER_THAN:
                        out = new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN, Long.toString(rhs));
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        out = new RangeConditionFilter(lhs.name(), Condition.GREATER_THAN_OR_EQUAL, Long.toString(rhs));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected operator " + original.operator());
                }
            }

            @Override
            public void visit(boolean rhs) {
                switch (preferred.operator()) {
                    case EQUALS:
                        out = new MatchFilter(lhs.name(), rhs);
                        break;
                    case NOT_EQUALS:
                        out = new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                        break;
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        out = WhereFilterFactory.getExpression(Strings.of(original));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected operator " + original.operator());
                }
            }

            @Override
            public void visit(Filter rhs) {
                out = WhereFilterFactory.getExpression(Strings.of(original));
            }

            @Override
            public void visit(ExpressionFunction function) {
                out = WhereFilterFactory.getExpression(Strings.of(original));
            }

            @Override
            public void visit(Literal value) {
                value.walk((Literal.Visitor) this);
            }

            @Override
            public void visit(RawString rawString) {
                out = WhereFilterFactory.getExpression(Strings.of(original));
            }
        }

        // Note for all remaining cases: since we are walking the preferred object, we know we don't have to handle
        // the case where rhs is column name.

        @Override
        public void visit(Literal lhs) {
            out = WhereFilterFactory.getExpression(Strings.of(original));
        }

        @Override
        public void visit(Filter lhs) {
            out = WhereFilterFactory.getExpression(Strings.of(original));
        }

        @Override
        public void visit(ExpressionFunction lhs) {
            out = WhereFilterFactory.getExpression(Strings.of(original));
        }

        @Override
        public void visit(RawString lhs) {
            out = WhereFilterFactory.getExpression(Strings.of(original));
        }
    }

    private static class ExpressionIsNullAdapter implements Expression.Visitor {

        public static WhereFilter of(Expression expression) {
            return expression.walk(new ExpressionIsNullAdapter(false)).out();
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

        public static WhereFilter of(ExpressionFunction function, boolean inverted) {
            return WhereFilterFactory
                    .getExpression(String.format(inverted ? "!isNull(%s)" : "isNull(%s)", Strings.of(function)));
        }

        public static WhereFilter of(RawString rawString, boolean inverted) {
            return WhereFilterFactory
                    .getExpression(String.format(inverted ? "!isNull(%s)" : "isNull(%s)", Strings.of(rawString)));
        }

        private final boolean inverted;
        private WhereFilter out;

        ExpressionIsNullAdapter(boolean inverted) {
            this.inverted = inverted;
        }

        public WhereFilter out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(Literal literal) {
            out = of(literal, inverted);
        }

        @Override
        public void visit(ColumnName columnName) {
            out = of(columnName, inverted);
        }

        @Override
        public void visit(Filter filter) {
            out = of(filter, inverted);
        }

        @Override
        public void visit(ExpressionFunction function) {
            out = of(function, inverted);
        }

        @Override
        public void visit(RawString rawString) {
            out = of(rawString, inverted);
        }
    }
}
