package io.deephaven.api.filter;

import io.deephaven.api.BuildableStyle;
import io.deephaven.api.value.Value;
import org.immutables.value.Value.Immutable;

import java.io.Serializable;

/**
 * Evaluates to true based on the specific {@link #operator() operator} applied to the {@link #lhs()
 * left-hand side} and {@link #rhs() right-hand side}.
 */
@Immutable
@BuildableStyle
public abstract class FilterCondition extends FilterBase implements Serializable {

    public enum Operator {
        /**
         * {@code lhs < rhs}
         */
        LESS_THAN,

        /**
         * {@code lhs <= rhs}
         */
        LESS_THAN_OR_EQUAL,

        /**
         * {@code lhs > rhs}
         */
        GREATER_THAN,

        /**
         * {@code lhs >= rhs}
         */
        GREATER_THAN_OR_EQUAL,

        /**
         * {@code lhs == rhs}
         */
        EQUALS,

        /**
         * {@code lhs != rhs}
         */
        NOT_EQUALS;

        public final FilterCondition of(Value lhs, Value rhs) {
            return FilterCondition.builder().operator(this).lhs(lhs).rhs(rhs).build();
        }
    }

    public static Builder builder() {
        return ImmutableFilterCondition.builder();
    }

    public static FilterCondition lt(Value lhs, Value rhs) {
        return Operator.LESS_THAN.of(lhs, rhs);
    }

    public static FilterCondition lte(Value lhs, Value rhs) {
        return Operator.LESS_THAN_OR_EQUAL.of(lhs, rhs);
    }

    public static FilterCondition gt(Value lhs, Value rhs) {
        return Operator.GREATER_THAN.of(lhs, rhs);
    }

    public static FilterCondition gte(Value lhs, Value rhs) {
        return Operator.GREATER_THAN_OR_EQUAL.of(lhs, rhs);
    }

    public static FilterCondition eq(Value lhs, Value rhs) {
        return Operator.EQUALS.of(lhs, rhs);
    }

    public static FilterCondition neq(Value lhs, Value rhs) {
        return Operator.NOT_EQUALS.of(lhs, rhs);
    }

    /**
     * The operator.
     *
     * @return the operator
     */
    public abstract Operator operator();

    /**
     * The left-hand side value.
     * 
     * @return the left-hand side value
     */
    public abstract Value lhs();

    /**
     * The right-hand side value.
     * 
     * @return the right-hand side value
     */
    public abstract Value rhs();

    @Override
    public final <V extends Filter.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {
        Builder operator(Operator operator);

        Builder lhs(Value lhs);

        Builder rhs(Value rhs);

        FilterCondition build();
    }
}
