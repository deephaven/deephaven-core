package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import io.deephaven.api.filter.FilterCondition.Operator;
import io.deephaven.api.value.Value;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterConditionTest {

    static final ColumnName FOO = ColumnName.of("Foo");
    static final ColumnName BAR = ColumnName.of("Bar");
    static final Value V42 = Value.of(42L);

    static final FilterCondition FOO_EQ_42 = FilterCondition.eq(FOO, V42);
    static final FilterCondition FOO_GT_42 = FilterCondition.gt(FOO, V42);
    static final FilterCondition FOO_GTE_42 = FilterCondition.gte(FOO, V42);
    static final FilterCondition FOO_LT_42 = FilterCondition.lt(FOO, V42);
    static final FilterCondition FOO_LTE_42 = FilterCondition.lte(FOO, V42);
    static final FilterCondition FOO_NEQ_42 = FilterCondition.neq(FOO, V42);

    static final FilterCondition FOO_EQ_BAR = FilterCondition.eq(FOO, BAR);
    static final FilterCondition FOO_GT_BAR = FilterCondition.gt(FOO, BAR);
    static final FilterCondition FOO_GTE_BAR = FilterCondition.gte(FOO, BAR);
    static final FilterCondition FOO_LT_BAR = FilterCondition.lt(FOO, BAR);
    static final FilterCondition FOO_LTE_BAR = FilterCondition.lte(FOO, BAR);
    static final FilterCondition FOO_NEQ_BAR = FilterCondition.neq(FOO, BAR);

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
    }

    @Test
    void columnAndLongStrings() {
        toString(FOO_EQ_42, "Foo == 42");
        toString(FOO_GT_42, "Foo > 42");
        toString(FOO_GTE_42, "Foo >= 42");
        toString(FOO_LT_42, "Foo < 42");
        toString(FOO_LTE_42, "Foo <= 42");
        toString(FOO_NEQ_42, "Foo != 42");
    }

    @Test
    void columnAndColumnStrings() {
        toString(FOO_EQ_BAR, "Foo == Bar");
        toString(FOO_GT_BAR, "Foo > Bar");
        toString(FOO_GTE_BAR, "Foo >= Bar");
        toString(FOO_LT_BAR, "Foo < Bar");
        toString(FOO_LTE_BAR, "Foo <= Bar");
        toString(FOO_NEQ_BAR, "Foo != Bar");
    }

    @Test
    void invert() {
        assertThat(FOO_EQ_42.invert()).isEqualTo(FOO_NEQ_42);
        assertThat(FOO_NEQ_42.invert()).isEqualTo(FOO_EQ_42);
        assertThat(FOO_GT_42.invert()).isEqualTo(FOO_LTE_42);
        assertThat(FOO_GTE_42.invert()).isEqualTo(FOO_LT_42);
        assertThat(FOO_LT_42.invert()).isEqualTo(FOO_GTE_42);
        assertThat(FOO_LTE_42.invert()).isEqualTo(FOO_GT_42);
    }

    @Test
    void transpose() {
        assertThat(FOO_EQ_42.transpose()).isEqualTo(FilterCondition.eq(V42, FOO));
        assertThat(FOO_NEQ_42.transpose()).isEqualTo(FilterCondition.neq(V42, FOO));
        assertThat(FOO_GT_42.transpose()).isEqualTo(FilterCondition.lt(V42, FOO));
        assertThat(FOO_GTE_42.transpose()).isEqualTo(FilterCondition.lte(V42, FOO));
        assertThat(FOO_LT_42.transpose()).isEqualTo(FilterCondition.gt(V42, FOO));
        assertThat(FOO_LTE_42.transpose()).isEqualTo(FilterCondition.gte(V42, FOO));

        assertThat(FilterCondition.eq(V42, FOO).transpose()).isEqualTo(FOO_EQ_42);
        assertThat(FilterCondition.neq(V42, FOO).transpose()).isEqualTo(FOO_NEQ_42);
        assertThat(FilterCondition.lt(V42, FOO).transpose()).isEqualTo(FOO_GT_42);
        assertThat(FilterCondition.lte(V42, FOO).transpose()).isEqualTo(FOO_GTE_42);
        assertThat(FilterCondition.gt(V42, FOO).transpose()).isEqualTo(FOO_LT_42);
        assertThat(FilterCondition.gte(V42, FOO).transpose()).isEqualTo(FOO_LTE_42);
    }

    @Test
    void maybeTranspose() {
        assertThat(FOO_EQ_42.maybeTranspose()).isEqualTo(FOO_EQ_42);
        assertThat(FOO_NEQ_42.maybeTranspose()).isEqualTo(FOO_NEQ_42);
        assertThat(FOO_GT_42.maybeTranspose()).isEqualTo(FOO_GT_42);
        assertThat(FOO_GTE_42.maybeTranspose()).isEqualTo(FOO_GTE_42);
        assertThat(FOO_LT_42.maybeTranspose()).isEqualTo(FOO_LT_42);
        assertThat(FOO_LTE_42.maybeTranspose()).isEqualTo(FOO_LTE_42);

        assertThat(FilterCondition.eq(V42, FOO).maybeTranspose()).isEqualTo(FOO_EQ_42);
        assertThat(FilterCondition.neq(V42, FOO).maybeTranspose()).isEqualTo(FOO_NEQ_42);
        assertThat(FilterCondition.lt(V42, FOO).maybeTranspose()).isEqualTo(FOO_GT_42);
        assertThat(FilterCondition.lte(V42, FOO).maybeTranspose()).isEqualTo(FOO_GTE_42);
        assertThat(FilterCondition.gt(V42, FOO).maybeTranspose()).isEqualTo(FOO_LT_42);
        assertThat(FilterCondition.gte(V42, FOO).maybeTranspose()).isEqualTo(FOO_LTE_42);
    }

    private static void toString(FilterCondition condition, String expected) {
        assertThat(toString(condition)).isEqualTo(expected);
    }

    private static String toString(FilterCondition filterCondition) {
        return Strings.of(filterCondition);
    }
}
