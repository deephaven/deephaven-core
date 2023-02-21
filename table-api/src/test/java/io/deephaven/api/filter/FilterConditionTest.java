/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import io.deephaven.api.filter.FilterComparison.Operator;
import io.deephaven.api.value.Value;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterConditionTest {

    static final ColumnName FOO = ColumnName.of("Foo");
    static final ColumnName BAR = ColumnName.of("Bar");
    static final Value V42 = Value.of(42L);
    static final RawString E42 = RawString.of("41 + 1");

    static final FilterComparison FOO_EQ_42 = FilterComparison.eq(FOO, V42);
    static final FilterComparison FOO_GT_42 = FilterComparison.gt(FOO, V42);
    static final FilterComparison FOO_GTE_42 = FilterComparison.gte(FOO, V42);
    static final FilterComparison FOO_LT_42 = FilterComparison.lt(FOO, V42);
    static final FilterComparison FOO_LTE_42 = FilterComparison.lte(FOO, V42);
    static final FilterComparison FOO_NEQ_42 = FilterComparison.neq(FOO, V42);

    static final FilterComparison FOO_EQ_BAR = FilterComparison.eq(FOO, BAR);
    static final FilterComparison FOO_GT_BAR = FilterComparison.gt(FOO, BAR);
    static final FilterComparison FOO_GTE_BAR = FilterComparison.gte(FOO, BAR);
    static final FilterComparison FOO_LT_BAR = FilterComparison.lt(FOO, BAR);
    static final FilterComparison FOO_LTE_BAR = FilterComparison.lte(FOO, BAR);
    static final FilterComparison FOO_NEQ_BAR = FilterComparison.neq(FOO, BAR);

    static final FilterComparison FOO_EQ_E42 = FilterComparison.eq(FOO, E42);
    static final FilterComparison FOO_GT_E42 = FilterComparison.gt(FOO, E42);
    static final FilterComparison FOO_GTE_E42 = FilterComparison.gte(FOO, E42);
    static final FilterComparison FOO_LT_E42 = FilterComparison.lt(FOO, E42);
    static final FilterComparison FOO_LTE_E42 = FilterComparison.lte(FOO, E42);
    static final FilterComparison FOO_NEQ_E42 = FilterComparison.neq(FOO, E42);

    @Test
    void lhs() {
        assertThat(FOO_EQ_42.lhs()).isEqualTo(FOO);
        assertThat(FOO_GT_42.lhs()).isEqualTo(FOO);
        assertThat(FOO_GTE_42.lhs()).isEqualTo(FOO);
        assertThat(FOO_LT_42.lhs()).isEqualTo(FOO);
        assertThat(FOO_LTE_42.lhs()).isEqualTo(FOO);
        assertThat(FOO_NEQ_42.lhs()).isEqualTo(FOO);

        assertThat(FOO_EQ_BAR.lhs()).isEqualTo(FOO);
        assertThat(FOO_GT_BAR.lhs()).isEqualTo(FOO);
        assertThat(FOO_GTE_BAR.lhs()).isEqualTo(FOO);
        assertThat(FOO_LT_BAR.lhs()).isEqualTo(FOO);
        assertThat(FOO_LTE_BAR.lhs()).isEqualTo(FOO);
        assertThat(FOO_NEQ_BAR.lhs()).isEqualTo(FOO);

        assertThat(FOO_EQ_E42.lhs()).isEqualTo(FOO);
        assertThat(FOO_GT_E42.lhs()).isEqualTo(FOO);
        assertThat(FOO_GTE_E42.lhs()).isEqualTo(FOO);
        assertThat(FOO_LT_E42.lhs()).isEqualTo(FOO);
        assertThat(FOO_LTE_E42.lhs()).isEqualTo(FOO);
        assertThat(FOO_NEQ_E42.lhs()).isEqualTo(FOO);
    }

    @Test
    void rhs() {
        assertThat(FOO_EQ_42.rhs()).isEqualTo(V42);
        assertThat(FOO_GT_42.rhs()).isEqualTo(V42);
        assertThat(FOO_GTE_42.rhs()).isEqualTo(V42);
        assertThat(FOO_LT_42.rhs()).isEqualTo(V42);
        assertThat(FOO_LTE_42.rhs()).isEqualTo(V42);
        assertThat(FOO_NEQ_42.rhs()).isEqualTo(V42);

        assertThat(FOO_EQ_BAR.rhs()).isEqualTo(BAR);
        assertThat(FOO_GT_BAR.rhs()).isEqualTo(BAR);
        assertThat(FOO_GTE_BAR.rhs()).isEqualTo(BAR);
        assertThat(FOO_LT_BAR.rhs()).isEqualTo(BAR);
        assertThat(FOO_LTE_BAR.rhs()).isEqualTo(BAR);
        assertThat(FOO_NEQ_BAR.rhs()).isEqualTo(BAR);

        assertThat(FOO_EQ_E42.rhs()).isEqualTo(E42);
        assertThat(FOO_GT_E42.rhs()).isEqualTo(E42);
        assertThat(FOO_GTE_E42.rhs()).isEqualTo(E42);
        assertThat(FOO_LT_E42.rhs()).isEqualTo(E42);
        assertThat(FOO_LTE_E42.rhs()).isEqualTo(E42);
        assertThat(FOO_NEQ_E42.rhs()).isEqualTo(E42);
    }

    @Test
    void operation() {
        assertThat(FOO_EQ_42.operator()).isEqualTo(Operator.EQUALS);
        assertThat(FOO_GT_42.operator()).isEqualTo(Operator.GREATER_THAN);
        assertThat(FOO_GTE_42.operator()).isEqualTo(Operator.GREATER_THAN_OR_EQUAL);
        assertThat(FOO_LT_42.operator()).isEqualTo(Operator.LESS_THAN);
        assertThat(FOO_LTE_42.operator()).isEqualTo(Operator.LESS_THAN_OR_EQUAL);
        assertThat(FOO_NEQ_42.operator()).isEqualTo(Operator.NOT_EQUALS);

        assertThat(FOO_EQ_BAR.operator()).isEqualTo(Operator.EQUALS);
        assertThat(FOO_GT_BAR.operator()).isEqualTo(Operator.GREATER_THAN);
        assertThat(FOO_GTE_BAR.operator()).isEqualTo(Operator.GREATER_THAN_OR_EQUAL);
        assertThat(FOO_LT_BAR.operator()).isEqualTo(Operator.LESS_THAN);
        assertThat(FOO_LTE_BAR.operator()).isEqualTo(Operator.LESS_THAN_OR_EQUAL);
        assertThat(FOO_NEQ_BAR.operator()).isEqualTo(Operator.NOT_EQUALS);

        assertThat(FOO_EQ_E42.operator()).isEqualTo(Operator.EQUALS);
        assertThat(FOO_GT_E42.operator()).isEqualTo(Operator.GREATER_THAN);
        assertThat(FOO_GTE_E42.operator()).isEqualTo(Operator.GREATER_THAN_OR_EQUAL);
        assertThat(FOO_LT_E42.operator()).isEqualTo(Operator.LESS_THAN);
        assertThat(FOO_LTE_E42.operator()).isEqualTo(Operator.LESS_THAN_OR_EQUAL);
        assertThat(FOO_NEQ_E42.operator()).isEqualTo(Operator.NOT_EQUALS);
    }

    @Test
    void columnAndLongStrings() {
        toString(FOO_EQ_42, "(Foo) == (42)");
        toString(FOO_GT_42, "(Foo) > (42)");
        toString(FOO_GTE_42, "(Foo) >= (42)");
        toString(FOO_LT_42, "(Foo) < (42)");
        toString(FOO_LTE_42, "(Foo) <= (42)");
        toString(FOO_NEQ_42, "(Foo) != (42)");
    }

    @Test
    void columnAndColumnStrings() {
        toString(FOO_EQ_BAR, "(Foo) == (Bar)");
        toString(FOO_GT_BAR, "(Foo) > (Bar)");
        toString(FOO_GTE_BAR, "(Foo) >= (Bar)");
        toString(FOO_LT_BAR, "(Foo) < (Bar)");
        toString(FOO_LTE_BAR, "(Foo) <= (Bar)");
        toString(FOO_NEQ_BAR, "(Foo) != (Bar)");
    }

    @Test
    void columnAndExpressionStrings() {
        toString(FOO_EQ_E42, "(Foo) == (41 + 1)");
        toString(FOO_GT_E42, "(Foo) > (41 + 1)");
        toString(FOO_GTE_E42, "(Foo) >= (41 + 1)");
        toString(FOO_LT_E42, "(Foo) < (41 + 1)");
        toString(FOO_LTE_E42, "(Foo) <= (41 + 1)");
        toString(FOO_NEQ_E42, "(Foo) != (41 + 1)");
    }

    @Test
    void invert() {
        assertThat(FOO_EQ_42.invert()).isEqualTo(FOO_NEQ_42);
        assertThat(FOO_NEQ_42.invert()).isEqualTo(FOO_EQ_42);
        assertThat(FOO_GT_42.invert()).isEqualTo(FOO_LTE_42);
        assertThat(FOO_GTE_42.invert()).isEqualTo(FOO_LT_42);
        assertThat(FOO_LT_42.invert()).isEqualTo(FOO_GTE_42);
        assertThat(FOO_LTE_42.invert()).isEqualTo(FOO_GT_42);

        assertThat(FOO_EQ_E42.invert()).isEqualTo(FOO_NEQ_E42);
        assertThat(FOO_NEQ_E42.invert()).isEqualTo(FOO_EQ_E42);
        assertThat(FOO_GT_E42.invert()).isEqualTo(FOO_LTE_E42);
        assertThat(FOO_GTE_E42.invert()).isEqualTo(FOO_LT_E42);
        assertThat(FOO_LT_E42.invert()).isEqualTo(FOO_GTE_E42);
        assertThat(FOO_LTE_E42.invert()).isEqualTo(FOO_GT_E42);
    }

    @Test
    void transpose() {
        assertThat(FOO_EQ_42.transpose()).isEqualTo(FilterComparison.eq(V42, FOO));
        assertThat(FOO_NEQ_42.transpose()).isEqualTo(FilterComparison.neq(V42, FOO));
        assertThat(FOO_GT_42.transpose()).isEqualTo(FilterComparison.lt(V42, FOO));
        assertThat(FOO_GTE_42.transpose()).isEqualTo(FilterComparison.lte(V42, FOO));
        assertThat(FOO_LT_42.transpose()).isEqualTo(FilterComparison.gt(V42, FOO));
        assertThat(FOO_LTE_42.transpose()).isEqualTo(FilterComparison.gte(V42, FOO));

        assertThat(FilterComparison.eq(V42, FOO).transpose()).isEqualTo(FOO_EQ_42);
        assertThat(FilterComparison.neq(V42, FOO).transpose()).isEqualTo(FOO_NEQ_42);
        assertThat(FilterComparison.lt(V42, FOO).transpose()).isEqualTo(FOO_GT_42);
        assertThat(FilterComparison.lte(V42, FOO).transpose()).isEqualTo(FOO_GTE_42);
        assertThat(FilterComparison.gt(V42, FOO).transpose()).isEqualTo(FOO_LT_42);
        assertThat(FilterComparison.gte(V42, FOO).transpose()).isEqualTo(FOO_LTE_42);
    }

    @Test
    void maybeTranspose() {
        assertThat(FOO_EQ_42.maybeTranspose()).isEqualTo(FOO_EQ_42);
        assertThat(FOO_NEQ_42.maybeTranspose()).isEqualTo(FOO_NEQ_42);
        assertThat(FOO_GT_42.maybeTranspose()).isEqualTo(FOO_GT_42);
        assertThat(FOO_GTE_42.maybeTranspose()).isEqualTo(FOO_GTE_42);
        assertThat(FOO_LT_42.maybeTranspose()).isEqualTo(FOO_LT_42);
        assertThat(FOO_LTE_42.maybeTranspose()).isEqualTo(FOO_LTE_42);

        assertThat(FilterComparison.eq(V42, FOO).maybeTranspose()).isEqualTo(FOO_EQ_42);
        assertThat(FilterComparison.neq(V42, FOO).maybeTranspose()).isEqualTo(FOO_NEQ_42);
        assertThat(FilterComparison.lt(V42, FOO).maybeTranspose()).isEqualTo(FOO_GT_42);
        assertThat(FilterComparison.lte(V42, FOO).maybeTranspose()).isEqualTo(FOO_GTE_42);
        assertThat(FilterComparison.gt(V42, FOO).maybeTranspose()).isEqualTo(FOO_LT_42);
        assertThat(FilterComparison.gte(V42, FOO).maybeTranspose()).isEqualTo(FOO_LTE_42);

        assertThat(FOO_EQ_E42.maybeTranspose()).isEqualTo(FOO_EQ_E42);
        assertThat(FOO_NEQ_E42.maybeTranspose()).isEqualTo(FOO_NEQ_E42);
        assertThat(FOO_GT_E42.maybeTranspose()).isEqualTo(FOO_GT_E42);
        assertThat(FOO_GTE_E42.maybeTranspose()).isEqualTo(FOO_GTE_E42);
        assertThat(FOO_LT_E42.maybeTranspose()).isEqualTo(FOO_LT_E42);
        assertThat(FOO_LTE_E42.maybeTranspose()).isEqualTo(FOO_LTE_E42);
    }

    private static void toString(FilterComparison condition, String expected) {
        assertThat(toString(condition)).isEqualTo(expected);
    }

    private static String toString(FilterComparison filterCondition) {
        return Strings.of(filterCondition);
    }
}
