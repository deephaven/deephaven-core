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
import io.deephaven.api.value.Literal;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;

import java.util.Objects;

class WhereFilterAdapter implements Filter.Visitor {
    private final boolean inverted;
    private WhereFilter out;

    WhereFilterAdapter(boolean inverted) {
        this.inverted = inverted;
    }

    public WhereFilter getOut() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(FilterComparison comparison) {
        out = FilterComparisonAdapter.of(inverted ? comparison.inverse() : comparison);
    }

    @Override
    public void visit(FilterNot not) {
        out = not.filter().walk(new WhereFilterAdapter(!inverted)).getOut();
    }

    @Override
    public void visit(FilterIsNull isNull) {
        out = isNull.expression().walk(new ExpressionIsNullAdapter(inverted)).out();
    }

    @Override
    public void visit(FilterIsNotNull isNotNull) {
        out = isNotNull.expression().walk(new ExpressionIsNullAdapter(!inverted)).out();
    }

    @Override
    public void visit(FilterOr ors) {
        if (inverted) {
            // !A && !B && ... && !Z
            out = ConjunctiveFilter.makeConjunctiveFilter(WhereFilter.fromInverted(ors.filters()));
        } else {
            // A || B || ... || Z
            out = DisjunctiveFilter.makeDisjunctiveFilter(WhereFilter.from(ors.filters()));
        }
    }

    @Override
    public void visit(FilterAnd ands) {
        if (inverted) {
            // !A || !B || ... || !Z
            out = DisjunctiveFilter.makeDisjunctiveFilter(WhereFilter.fromInverted(ands.filters()));
        } else {
            // A && B && ... && Z
            out = ConjunctiveFilter.makeConjunctiveFilter(WhereFilter.from(ands.filters()));
        }
    }

    @Override
    public void visit(boolean literal) {
        if (inverted ^ literal) {
            out = WhereFilterFactory.getExpression("true");
        } else {
            out = WhereFilterFactory.getExpression("false");
        }
    }

    @Override
    public void visit(RawString rawString) {
        if (inverted) {
            out = WhereFilterFactory.getExpression(String.format("!(%s)", rawString.value()));
        } else {
            out = WhereFilterFactory.getExpression(rawString.value());
        }
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
                    case LESS_THAN:
                        out = IntRangeFilter.lt(lhs.name(), rhs);
                        break;
                    case LESS_THAN_OR_EQUAL:
                        out = IntRangeFilter.lte(lhs.name(), rhs);
                        break;
                    case GREATER_THAN:
                        out = IntRangeFilter.gt(lhs.name(), rhs);
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        out = IntRangeFilter.gte(lhs.name(), rhs);
                        break;
                    case EQUALS:
                        out = new MatchFilter(lhs.name(), rhs);
                        break;
                    case NOT_EQUALS:
                        out = new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected operator " + original.operator());
                }
            }

            @Override
            public void visit(long rhs) {
                switch (preferred.operator()) {
                    case LESS_THAN:
                        out = LongRangeFilter.lt(lhs.name(), rhs);
                        break;
                    case LESS_THAN_OR_EQUAL:
                        out = LongRangeFilter.lte(lhs.name(), rhs);
                        break;
                    case GREATER_THAN:
                        out = LongRangeFilter.gt(lhs.name(), rhs);
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        out = LongRangeFilter.gte(lhs.name(), rhs);
                        break;
                    case EQUALS:
                        out = new MatchFilter(lhs.name(), rhs);
                        break;
                    case NOT_EQUALS:
                        out = new MatchFilter(MatchType.Inverted, lhs.name(), rhs);
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
            // isNotNull(literal) is always true
            // isNull(literal) is always false
            out = inverted ? WhereFilterFactory.getExpression("true") : WhereFilterFactory.getExpression("false");
        }

        @Override
        public void visit(ColumnName columnName) {
            out = new MatchFilter(inverted ? MatchType.Inverted : MatchType.Regular, columnName.name(),
                    new Object[] {null});
        }

        @Override
        public void visit(Filter filter) {
            // A filter application will always evaluate to true or false, never null
            // isNotNull(filter(...)) is always true
            // isNull(filter(...)) is always false
            out = inverted ? WhereFilterFactory.getExpression("true") : WhereFilterFactory.getExpression("false");
        }

        @Override
        public void visit(ExpressionFunction function) {
            out = inverted ? WhereFilterFactory.getExpression(Strings.of(Filter.isNotNull(function)))
                    : WhereFilterFactory.getExpression(Strings.of(Filter.isNull(function)));
        }

        @Override
        public void visit(RawString rawString) {
            out = inverted ? WhereFilterFactory.getExpression(Strings.of(Filter.isNotNull(rawString)))
                    : WhereFilterFactory.getExpression(Strings.of(Filter.isNull(rawString)));
        }
    }
}
