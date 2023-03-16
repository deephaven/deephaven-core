/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.ExpressionFunction;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.value.Literal;
import junit.framework.TestCase;

import static org.assertj.core.api.Assertions.assertThat;

public class WhereFilterTest extends TestCase {

    private static final ColumnName FOO = ColumnName.of("Foo");
    private static final ColumnName BAR = ColumnName.of("Bar");
    private static final Literal V42 = Literal.of(42L);

    public void testFooIsTrue() {
        expect(Filter.isTrue(FOO), MatchFilter.class, "Foo in [true]");
    }

    public void testFooIsFalse() {
        expect(Filter.isFalse(FOO), MatchFilter.class, "Foo in [false]");
    }

    public void testFooIsNotTrue() {
        // This is *not* logically equivalent to "Foo in [false]"
        expect(Filter.not(Filter.isTrue(FOO)), MatchFilter.class, "Foo not in [true]");
    }

    public void testFooIsNotFalse() {
        // This is *not* logically equivalent to "Foo in [true]"
        expect(Filter.not(Filter.isFalse(FOO)), MatchFilter.class, "Foo not in [false]");
    }

    public void testFooIsNull() {
        expect(Filter.isNull(FOO), MatchFilter.class, "Foo in [null]");
    }

    public void testFooIsNotNull() {
        expect(Filter.isNotNull(FOO), MatchFilter.class, "Foo not in [null]");
    }

    public void testFooAndBar() {
        expect(Filter.and(Filter.isTrue(FOO), Filter.isTrue(BAR)), ConjunctiveFilter.class,
                "ConjunctiveFilter([Foo in [true], Bar in [true]])");
        expect(Filter.not(Filter.and(Filter.isTrue(FOO), Filter.isTrue(BAR))), DisjunctiveFilter.class,
                "DisjunctiveFilter([Foo not in [true], Bar not in [true]])");
    }

    public void testFooOrBar() {
        expect(Filter.or(Filter.isTrue(FOO), Filter.isTrue(BAR)), DisjunctiveFilter.class,
                "DisjunctiveFilter([Foo in [true], Bar in [true]])");
        expect(Filter.not(Filter.or(Filter.isTrue(FOO), Filter.isTrue(BAR))), ConjunctiveFilter.class,
                "ConjunctiveFilter([Foo not in [true], Bar not in [true]])");
    }

    public void testRawString() {
        expect(RawString.of("X * y > foo(Z)"), ConditionFilter.class, "X * y > foo(Z)");
        expect(Filter.not(RawString.of("X * y > foo(Z)")), ConditionFilter.class, "!(X * y > foo(Z))");
    }

    public void testEq() {
        expect(FilterComparison.eq(FOO, V42), MatchFilter.class, "Foo in [42]");
        expect(FilterComparison.eq(V42, FOO), MatchFilter.class, "Foo in [42]");
        expect(FilterComparison.eq(FOO, BAR), ConditionFilter.class, "(Foo) == (Bar)");

        expect(Filter.not(FilterComparison.eq(FOO, V42)), MatchFilter.class, "Foo not in [42]");
        expect(Filter.not(FilterComparison.eq(V42, FOO)), MatchFilter.class, "Foo not in [42]");
        expect(Filter.not(FilterComparison.eq(FOO, BAR)), ConditionFilter.class, "(Foo) != (Bar)");
    }

    public void testNeq() {
        expect(FilterComparison.neq(FOO, V42), MatchFilter.class, "Foo not in [42]");
        expect(FilterComparison.neq(V42, FOO), MatchFilter.class, "Foo not in [42]");
        expect(FilterComparison.neq(FOO, BAR), ConditionFilter.class, "(Foo) != (Bar)");

        expect(Filter.not(FilterComparison.neq(FOO, V42)), MatchFilter.class, "Foo in [42]");
        expect(Filter.not(FilterComparison.neq(V42, FOO)), MatchFilter.class, "Foo in [42]");
        expect(Filter.not(FilterComparison.neq(FOO, BAR)), ConditionFilter.class, "(Foo) == (Bar)");
    }

    public void testGt() {
        expect(FilterComparison.gt(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        expect(FilterComparison.gt(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        expect(FilterComparison.gt(FOO, BAR), ConditionFilter.class, "(Foo) > (Bar)");

        expect(Filter.not(FilterComparison.gt(FOO, V42)), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        expect(Filter.not(FilterComparison.gt(V42, FOO)), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        expect(Filter.not(FilterComparison.gt(FOO, BAR)), ConditionFilter.class, "(Foo) <= (Bar)");
    }

    public void testGte() {
        expect(FilterComparison.gte(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        expect(FilterComparison.gte(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        expect(FilterComparison.gte(FOO, BAR), ConditionFilter.class, "(Foo) >= (Bar)");

        expect(Filter.not(FilterComparison.gte(FOO, V42)), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        expect(Filter.not(FilterComparison.gte(V42, FOO)), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        expect(Filter.not(FilterComparison.gte(FOO, BAR)), ConditionFilter.class, "(Foo) < (Bar)");
    }

    public void testLt() {
        expect(FilterComparison.lt(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        expect(FilterComparison.lt(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        expect(FilterComparison.lt(FOO, BAR), ConditionFilter.class, "(Foo) < (Bar)");

        expect(Filter.not(FilterComparison.lt(FOO, V42)), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        expect(Filter.not(FilterComparison.lt(V42, FOO)), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        expect(Filter.not(FilterComparison.lt(FOO, BAR)), ConditionFilter.class, "(Foo) >= (Bar)");
    }

    public void testLte() {
        expect(FilterComparison.lte(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        expect(FilterComparison.lte(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        expect(FilterComparison.lte(FOO, BAR), ConditionFilter.class, "(Foo) <= (Bar)");

        expect(Filter.not(FilterComparison.lte(FOO, V42)), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        expect(Filter.not(FilterComparison.lte(V42, FOO)), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        expect(Filter.not(FilterComparison.lte(FOO, BAR)), ConditionFilter.class, "(Foo) > (Bar)");
    }

    public void testFunctionIsTrue() {
        expect(Filter.isTrue(ExpressionFunction.builder().name("someMethod").addArguments(FOO, BAR).build()),
                ConditionFilter.class, "(someMethod(Foo, Bar)) == (true)");
        expect(Filter
                .not(Filter.isTrue(ExpressionFunction.builder().name("someMethod").addArguments(FOO, BAR).build())),
                ConditionFilter.class, "(someMethod(Foo, Bar)) != (true)");
    }

    public void testFunctionIsFalse() {
        expect(Filter.isFalse(ExpressionFunction.builder().name("someMethod").addArguments(FOO, BAR).build()),
                ConditionFilter.class, "(someMethod(Foo, Bar)) == (false)");
        expect(Filter
                .not(Filter.isFalse(ExpressionFunction.builder().name("someMethod").addArguments(FOO, BAR).build())),
                ConditionFilter.class, "(someMethod(Foo, Bar)) != (false)");
    }

    public void testFunctionIsNull() {
        expect(Filter.isNull(ExpressionFunction.builder().name("someMethod").addArguments(FOO, BAR).build()),
                ConditionFilter.class, "isNull(someMethod(Foo, Bar))");
        expect(Filter
                .not(Filter.isNull(ExpressionFunction.builder().name("someMethod").addArguments(FOO, BAR).build())),
                ConditionFilter.class, "!isNull(someMethod(Foo, Bar))");
    }

    public void testFunctionIsNotNull() {
        expect(Filter.isNotNull(ExpressionFunction.builder().name("someMethod").addArguments(FOO, BAR).build()),
                ConditionFilter.class, "!isNull(someMethod(Foo, Bar))");
        expect(Filter
                .not(Filter.isNotNull(ExpressionFunction.builder().name("someMethod").addArguments(FOO, BAR).build())),
                ConditionFilter.class, "isNull(someMethod(Foo, Bar))");
    }

    public void testLiteralIsTrue() {
        expect(Filter.isTrue(Literal.of(42)), ConditionFilter.class, "((int)42) == (true)");
        expect(Filter.not(Filter.isTrue(Literal.of(42))), ConditionFilter.class, "((int)42) != (true)");
    }

    public void testLiteralIsFalse() {
        expect(Filter.isFalse(Literal.of(42)), ConditionFilter.class, "((int)42) == (false)");
        expect(Filter.not(Filter.isFalse(Literal.of(42))), ConditionFilter.class, "((int)42) != (false)");
    }

    public void testLiteralIsNull() {
        expect(Filter.isNull(Literal.of(42)), ConditionFilter.class, "isNull((int)42)");
        expect(Filter.not(Filter.isNull(Literal.of(42))), ConditionFilter.class, "!isNull((int)42)");
    }

    public void testLiteralIsNotNull() {
        expect(Filter.isNotNull(Literal.of(42)), ConditionFilter.class, "!isNull((int)42)");
        expect(Filter.not(Filter.isNotNull(Literal.of(42))), ConditionFilter.class, "isNull((int)42)");
    }

    public void testFilterTrue() {
        assertThat(WhereFilter.of(Filter.ofTrue())).isEqualTo(WhereAllFilter.INSTANCE);
    }

    public void testFilterFalse() {
        assertThat(WhereFilter.of(Filter.ofFalse())).isEqualTo(WhereNoneFilter.INSTANCE);
    }

    private static void expect(Filter filter, Class<? extends WhereFilter> clazz, String expected) {
        WhereFilter impl = WhereFilter.of(filter);
        assertThat(impl).isInstanceOf(clazz);
        // WhereFilter doesn't necessary implement equals, so we need to use the string repr
        assertThat(impl.toString()).isEqualTo(expected);
    }
}
