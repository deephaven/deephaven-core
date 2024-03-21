//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.FilterComparison.Operator;
import io.deephaven.api.literal.Literal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterComparisionTest {

    static final ColumnName FOO = ColumnName.of("Foo");
    static final ColumnName BAR = ColumnName.of("Bar");
    static final Literal V42 = Literal.of(42L);
    static final RawString E42 = RawString.of("41 + 1");

    static final FilterComparison FOO_EQ_42 = FilterComparison.eq(FOO, V42);
    static final FilterComparison FOO_GT_42 = FilterComparison.gt(FOO, V42);
    static final FilterComparison FOO_GTE_42 = FilterComparison.geq(FOO, V42);
    static final FilterComparison FOO_LT_42 = FilterComparison.lt(FOO, V42);
    static final FilterComparison FOO_LTE_42 = FilterComparison.leq(FOO, V42);
    static final FilterComparison FOO_NEQ_42 = FilterComparison.neq(FOO, V42);

    static final FilterComparison FOO_EQ_BAR = FilterComparison.eq(FOO, BAR);
    static final FilterComparison FOO_GT_BAR = FilterComparison.gt(FOO, BAR);
    static final FilterComparison FOO_GTE_BAR = FilterComparison.geq(FOO, BAR);
    static final FilterComparison FOO_LT_BAR = FilterComparison.lt(FOO, BAR);
    static final FilterComparison FOO_LTE_BAR = FilterComparison.leq(FOO, BAR);
    static final FilterComparison FOO_NEQ_BAR = FilterComparison.neq(FOO, BAR);

    static final FilterComparison FOO_EQ_E42 = FilterComparison.eq(FOO, E42);
    static final FilterComparison FOO_GT_E42 = FilterComparison.gt(FOO, E42);
    static final FilterComparison FOO_GTE_E42 = FilterComparison.geq(FOO, E42);
    static final FilterComparison FOO_LT_E42 = FilterComparison.lt(FOO, E42);
    static final FilterComparison FOO_LTE_E42 = FilterComparison.leq(FOO, E42);
    static final FilterComparison FOO_NEQ_E42 = FilterComparison.neq(FOO, E42);

    @Test
    void properties() {
        checkProperties(FOO_EQ_42, FOO, Operator.EQUALS, V42, "Foo == 42L", FOO_NEQ_42);
        checkProperties(FOO_GT_42, FOO, Operator.GREATER_THAN, V42, "Foo > 42L", FOO_LTE_42);
        checkProperties(FOO_GTE_42, FOO, Operator.GREATER_THAN_OR_EQUAL, V42, "Foo >= 42L", FOO_LT_42);
        checkProperties(FOO_LT_42, FOO, Operator.LESS_THAN, V42, "Foo < 42L", FOO_GTE_42);
        checkProperties(FOO_LTE_42, FOO, Operator.LESS_THAN_OR_EQUAL, V42, "Foo <= 42L", FOO_GT_42);
        checkProperties(FOO_NEQ_42, FOO, Operator.NOT_EQUALS, V42, "Foo != 42L", FOO_EQ_42);

        checkProperties(FOO_EQ_BAR, FOO, Operator.EQUALS, BAR, "Foo == Bar", FOO_NEQ_BAR);
        checkProperties(FOO_GT_BAR, FOO, Operator.GREATER_THAN, BAR, "Foo > Bar", FOO_LTE_BAR);
        checkProperties(FOO_GTE_BAR, FOO, Operator.GREATER_THAN_OR_EQUAL, BAR, "Foo >= Bar", FOO_LT_BAR);
        checkProperties(FOO_LT_BAR, FOO, Operator.LESS_THAN, BAR, "Foo < Bar", FOO_GTE_BAR);
        checkProperties(FOO_LTE_BAR, FOO, Operator.LESS_THAN_OR_EQUAL, BAR, "Foo <= Bar", FOO_GT_BAR);
        checkProperties(FOO_NEQ_BAR, FOO, Operator.NOT_EQUALS, BAR, "Foo != Bar", FOO_EQ_BAR);

        checkProperties(FOO_EQ_E42, FOO, Operator.EQUALS, E42, "Foo == (41 + 1)", FOO_NEQ_E42);
        checkProperties(FOO_GT_E42, FOO, Operator.GREATER_THAN, E42, "Foo > (41 + 1)", FOO_LTE_E42);
        checkProperties(FOO_GTE_E42, FOO, Operator.GREATER_THAN_OR_EQUAL, E42, "Foo >= (41 + 1)", FOO_LT_E42);
        checkProperties(FOO_LT_E42, FOO, Operator.LESS_THAN, E42, "Foo < (41 + 1)", FOO_GTE_E42);
        checkProperties(FOO_LTE_E42, FOO, Operator.LESS_THAN_OR_EQUAL, E42, "Foo <= (41 + 1)", FOO_GT_E42);
        checkProperties(FOO_NEQ_E42, FOO, Operator.NOT_EQUALS, E42, "Foo != (41 + 1)", FOO_EQ_E42);
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
        assertThat(FOO_GTE_42.transpose()).isEqualTo(FilterComparison.leq(V42, FOO));
        assertThat(FOO_LT_42.transpose()).isEqualTo(FilterComparison.gt(V42, FOO));
        assertThat(FOO_LTE_42.transpose()).isEqualTo(FilterComparison.geq(V42, FOO));

        assertThat(FilterComparison.eq(V42, FOO).transpose()).isEqualTo(FOO_EQ_42);
        assertThat(FilterComparison.neq(V42, FOO).transpose()).isEqualTo(FOO_NEQ_42);
        assertThat(FilterComparison.lt(V42, FOO).transpose()).isEqualTo(FOO_GT_42);
        assertThat(FilterComparison.leq(V42, FOO).transpose()).isEqualTo(FOO_GTE_42);
        assertThat(FilterComparison.gt(V42, FOO).transpose()).isEqualTo(FOO_LT_42);
        assertThat(FilterComparison.geq(V42, FOO).transpose()).isEqualTo(FOO_LTE_42);
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
        assertThat(FilterComparison.leq(V42, FOO).maybeTranspose()).isEqualTo(FOO_GTE_42);
        assertThat(FilterComparison.gt(V42, FOO).maybeTranspose()).isEqualTo(FOO_LT_42);
        assertThat(FilterComparison.geq(V42, FOO).maybeTranspose()).isEqualTo(FOO_LTE_42);

        assertThat(FOO_EQ_E42.maybeTranspose()).isEqualTo(FOO_EQ_E42);
        assertThat(FOO_NEQ_E42.maybeTranspose()).isEqualTo(FOO_NEQ_E42);
        assertThat(FOO_GT_E42.maybeTranspose()).isEqualTo(FOO_GT_E42);
        assertThat(FOO_GTE_E42.maybeTranspose()).isEqualTo(FOO_GTE_E42);
        assertThat(FOO_LT_E42.maybeTranspose()).isEqualTo(FOO_LT_E42);
        assertThat(FOO_LTE_E42.maybeTranspose()).isEqualTo(FOO_LTE_E42);
    }

    private static void checkProperties(FilterComparison comparison, Expression lhs, Operator operator, Expression rhs,
            String expected, FilterComparison expectedInverse) {
        assertThat(comparison.lhs()).isEqualTo(lhs);
        assertThat(comparison.operator()).isEqualTo(operator);
        assertThat(comparison.rhs()).isEqualTo(rhs);
        assertThat(Strings.of(comparison)).isEqualTo(expected);
        assertThat(comparison.invert()).isEqualTo(expectedInverse);
    }
}
