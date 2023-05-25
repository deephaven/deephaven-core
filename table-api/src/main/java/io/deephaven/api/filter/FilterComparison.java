/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Immutable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Evaluates to true based on the specific {@link #operator() operator} applied to the {@link #lhs() left-hand side} and
 * {@link #rhs() right-hand side} {@link Expression expressions}.
 */
@Immutable
@BuildableStyle
public abstract class FilterComparison extends FilterBase implements Serializable {

    public enum Operator {
        /**
         * {@code lhs < rhs}
         */
        LESS_THAN("<"),

        /**
         * {@code lhs <= rhs}
         */
        LESS_THAN_OR_EQUAL("<="),

        /**
         * {@code lhs > rhs}
         */
        GREATER_THAN(">"),

        /**
         * {@code lhs >= rhs}
         */
        GREATER_THAN_OR_EQUAL(">="),

        /**
         * {@code lhs == rhs}
         */
        EQUALS("=="),

        /**
         * {@code lhs != rhs}
         */
        NOT_EQUALS("!=");

        private final String javaOperator;

        Operator(String javaOperator) {
            this.javaOperator = Objects.requireNonNull(javaOperator);
        }

        public String javaOperator() {
            return javaOperator;
        }

        public FilterComparison of(Expression lhs, Expression rhs) {
            return FilterComparison.builder().operator(this).lhs(lhs).rhs(rhs).build();
        }

        /**
         * The inverted, or negated, operator.
         *
         * @return the inverted operator
         */
        public Operator invert() {
            switch (this) {
                case LESS_THAN:
                    return GREATER_THAN_OR_EQUAL;
                case LESS_THAN_OR_EQUAL:
                    return GREATER_THAN;
                case GREATER_THAN:
                    return LESS_THAN_OR_EQUAL;
                case GREATER_THAN_OR_EQUAL:
                    return LESS_THAN;
                case EQUALS:
                    return NOT_EQUALS;
                case NOT_EQUALS:
                    return EQUALS;
                default:
                    throw new IllegalStateException("Unexpected operator " + this);
            }
        }

        /**
         * The transposed, or flipped, operator.
         *
         * @return the transposed operator
         */
        public Operator transpose() {
            switch (this) {
                case LESS_THAN:
                    return GREATER_THAN;
                case LESS_THAN_OR_EQUAL:
                    return GREATER_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return LESS_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
                case EQUALS:
                    return EQUALS;
                case NOT_EQUALS:
                    return NOT_EQUALS;
                default:
                    throw new IllegalStateException("Unexpected operator " + this);
            }
        }
    }

    public static Builder builder() {
        return ImmutableFilterComparison.builder();
    }

    public static FilterComparison lt(Expression lhs, Expression rhs) {
        return Operator.LESS_THAN.of(lhs, rhs);
    }

    public static FilterComparison leq(Expression lhs, Expression rhs) {
        return Operator.LESS_THAN_OR_EQUAL.of(lhs, rhs);
    }

    public static FilterComparison gt(Expression lhs, Expression rhs) {
        return Operator.GREATER_THAN.of(lhs, rhs);
    }

    public static FilterComparison geq(Expression lhs, Expression rhs) {
        return Operator.GREATER_THAN_OR_EQUAL.of(lhs, rhs);
    }

    public static FilterComparison eq(Expression lhs, Expression rhs) {
        return Operator.EQUALS.of(lhs, rhs);
    }

    public static FilterComparison neq(Expression lhs, Expression rhs) {
        return Operator.NOT_EQUALS.of(lhs, rhs);
    }

    /**
     * The operator.
     *
     * @return the operator
     */
    public abstract Operator operator();

    /**
     * The left-hand side expression.
     * 
     * @return the left-hand side expression
     */
    public abstract Expression lhs();

    /**
     * The right-hand side expression.
     * 
     * @return the right-hand side expression
     */
    public abstract Expression rhs();

    /**
     * The logically equivalent transposed filter.
     *
     * <p>
     * Equivalent to {@code operator().transpose().of(rhs(), lhs())}.
     *
     * <p>
     * Note: while logically equivalent, a transposed filter condition does not equal {@code this}.
     *
     * @return the transposed filter
     */
    public final FilterComparison transpose() {
        return operator().transpose().of(rhs(), lhs());
    }

    /**
     * {@link #transpose() Transpose} the filter if the {@link #lhs() left-hand side} is not a {@link ColumnName} and
     * the {@link #rhs() right-hand side} is a {@link ColumnName}.
     *
     * <p>
     * Useful in cases where a visitor wants to walk the sides, and prefers to have a {@link ColumnName} on the
     * {@link #lhs() left-hand side}.
     *
     * @return the filter, potentially transposed
     */
    public final FilterComparison maybeTranspose() {
        if (lhs() instanceof ColumnName) {
            return this;
        }
        if (rhs() instanceof ColumnName) {
            return transpose();
        }
        return this;
    }

    /**
     * The logically inversion of {@code this}.
     *
     * @return the inverted filter
     */
    @Override
    public final FilterComparison invert() {
        return operator().invert().of(lhs(), rhs());
    }

    @Override
    public final <T> T walk(Filter.Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {
        Builder operator(Operator operator);

        Builder lhs(Expression lhs);

        Builder rhs(Expression rhs);

        FilterComparison build();
    }
}
