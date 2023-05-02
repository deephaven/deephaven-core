/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
import junit.framework.TestCase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class WhereFilterTest extends TestCase {

    private static final ColumnName FOO = ColumnName.of("Foo");
    private static final ColumnName BAR = ColumnName.of("Bar");
    private static final ColumnName BAZ = ColumnName.of("Baz");
    private static final Literal V42 = Literal.of(42L);

    public void testFoo() {
        expect(FOO, ConditionFilter.class, "!isNull(Foo) && Foo");
    }

    public void testNotFoo() {
        opposite(FOO, ConditionFilter.class, "isNull(Foo) || !Foo");
    }

    public void testFooIsTrue() {
        expect(Filter.isTrue(FOO), MatchFilter.class, "Foo in [true]");
    }

    public void testFooIsFalse() {
        expect(Filter.isFalse(FOO), MatchFilter.class, "Foo in [false]");
    }

    public void testFooIsNotTrue() {
        // This is *not* logically equivalent to "Foo in [false]"
        // since we are really dealing with true, false, and null
        opposite(Filter.isTrue(FOO), MatchFilter.class, "Foo not in [true]");
    }

    public void testFooIsNotFalse() {
        // This is *not* logically equivalent to "Foo in [true]"
        // since we are really dealing with true, false, and null
        opposite(Filter.isFalse(FOO), MatchFilter.class, "Foo not in [false]");
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
        opposite(Filter.and(Filter.isTrue(FOO), Filter.isTrue(BAR)), DisjunctiveFilter.class,
                "DisjunctiveFilter([Foo not in [true], Bar not in [true]])");
    }

    public void testFooOrBar() {
        expect(Filter.or(Filter.isTrue(FOO), Filter.isTrue(BAR)), DisjunctiveFilter.class,
                "DisjunctiveFilter([Foo in [true], Bar in [true]])");
        opposite(Filter.or(Filter.isTrue(FOO), Filter.isTrue(BAR)), ConjunctiveFilter.class,
                "ConjunctiveFilter([Foo not in [true], Bar not in [true]])");
    }

    public void testRawString() {
        expect(RawString.of("X * y > foo(Z)"), ConditionFilter.class, "X * y > foo(Z)");
        opposite(RawString.of("X * y > foo(Z)"), ConditionFilter.class, "!(X * y > foo(Z))");
    }

    public void testEq() {
        expect(FilterComparison.eq(FOO, V42), MatchFilter.class, "Foo in [42]");
        expect(FilterComparison.eq(V42, FOO), MatchFilter.class, "Foo in [42]");
        expect(FilterComparison.eq(FOO, BAR), ConditionFilter.class, "Foo == Bar");

        opposite(FilterComparison.eq(FOO, V42), MatchFilter.class, "Foo not in [42]");
        opposite(FilterComparison.eq(V42, FOO), MatchFilter.class, "Foo not in [42]");
        opposite(FilterComparison.eq(FOO, BAR), ConditionFilter.class, "Foo != Bar");
    }

    public void testNeq() {
        expect(FilterComparison.neq(FOO, V42), MatchFilter.class, "Foo not in [42]");
        expect(FilterComparison.neq(V42, FOO), MatchFilter.class, "Foo not in [42]");
        expect(FilterComparison.neq(FOO, BAR), ConditionFilter.class, "Foo != Bar");

        opposite(FilterComparison.neq(FOO, V42), MatchFilter.class, "Foo in [42]");
        opposite(FilterComparison.neq(V42, FOO), MatchFilter.class, "Foo in [42]");
        opposite(FilterComparison.neq(FOO, BAR), ConditionFilter.class, "Foo == Bar");
    }

    public void testGt() {
        expect(FilterComparison.gt(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        expect(FilterComparison.gt(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        expect(FilterComparison.gt(FOO, BAR), ConditionFilter.class, "Foo > Bar");

        opposite(FilterComparison.gt(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        opposite(FilterComparison.gt(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        opposite(FilterComparison.gt(FOO, BAR), ConditionFilter.class, "Foo <= Bar");
    }

    public void testGte() {
        expect(FilterComparison.gte(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        expect(FilterComparison.gte(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        expect(FilterComparison.gte(FOO, BAR), ConditionFilter.class, "Foo >= Bar");

        opposite(FilterComparison.gte(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        opposite(FilterComparison.gte(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        opposite(FilterComparison.gte(FOO, BAR), ConditionFilter.class, "Foo < Bar");
    }

    public void testLt() {
        expect(FilterComparison.lt(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        expect(FilterComparison.lt(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        expect(FilterComparison.lt(FOO, BAR), ConditionFilter.class, "Foo < Bar");

        opposite(FilterComparison.lt(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        opposite(FilterComparison.lt(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        opposite(FilterComparison.lt(FOO, BAR), ConditionFilter.class, "Foo >= Bar");
    }

    public void testLte() {
        expect(FilterComparison.lte(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        expect(FilterComparison.lte(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        expect(FilterComparison.lte(FOO, BAR), ConditionFilter.class, "Foo <= Bar");

        opposite(FilterComparison.lte(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        opposite(FilterComparison.lte(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        opposite(FilterComparison.lte(FOO, BAR), ConditionFilter.class, "Foo > Bar");
    }

    public void testFunction() {
        expect(Function.of("someMethod"), ConditionFilter.class, "someMethod()");
        expect(Filter.not(Function.of("someMethod")), ConditionFilter.class, "!someMethod()");

        expect(Function.of("someMethod", FOO), ConditionFilter.class, "someMethod(Foo)");
        expect(Filter.not(Function.of("someMethod", FOO)), ConditionFilter.class, "!someMethod(Foo)");

        expect(Function.of("someMethod", FOO, BAR), ConditionFilter.class, "someMethod(Foo, Bar)");
        expect(Filter.not(Function.of("someMethod", FOO, BAR)), ConditionFilter.class, "!someMethod(Foo, Bar)");
    }

    public void testFunctionIsNull() {
        expect(Filter.isNull(Function.of("someMethod", FOO, BAR)), ConditionFilter.class,
                "isNull(someMethod(Foo, Bar))");
        expect(Filter.not(Filter.isNull(Function.of("someMethod", FOO, BAR))), ConditionFilter.class,
                "!isNull(someMethod(Foo, Bar))");
    }

    public void testFunctionIsNotNull() {
        expect(Filter.isNotNull(Function.of("someMethod", FOO, BAR)), ConditionFilter.class,
                "!isNull(someMethod(Foo, Bar))");
        expect(Filter.not(Filter.isNotNull(Function.of("someMethod", FOO, BAR))), ConditionFilter.class,
                "isNull(someMethod(Foo, Bar))");
    }

    public void testLiteralIsTrue() {
        expect(Filter.isTrue(Literal.of(42)), ConditionFilter.class, "(int)42 == true");
        opposite(Filter.isTrue(Literal.of(42)), ConditionFilter.class, "(int)42 != true");
    }

    public void testLiteralIsFalse() {
        expect(Filter.isFalse(Literal.of(42)), ConditionFilter.class, "(int)42 == false");
        opposite(Filter.isFalse(Literal.of(42)), ConditionFilter.class, "(int)42 != false");
    }

    public void testLiteralIsNull() {
        expect(Filter.isNull(Literal.of(42)), ConditionFilter.class, "isNull((int)42)");
        opposite(Filter.isNull(Literal.of(42)), ConditionFilter.class, "!isNull((int)42)");
    }

    public void testLiteralIsNotNull() {
        expect(Filter.isNotNull(Literal.of(42)), ConditionFilter.class, "!isNull((int)42)");
        opposite(Filter.isNotNull(Literal.of(42)), ConditionFilter.class, "isNull((int)42)");
    }

    public void testFilterTrue() {
        try {
            WhereFilter.of(Filter.ofTrue());
            failBecauseExceptionWasNotThrown(UnsupportedOperationException.class);
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    public void testFilterFalse() {
        try {
            WhereFilter.of(Filter.ofFalse());
            failBecauseExceptionWasNotThrown(UnsupportedOperationException.class);
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    private static void expect(Filter filter, Class<? extends WhereFilter> clazz, String expected) {
        WhereFilter impl = WhereFilter.of(filter);
        assertThat(impl).isInstanceOf(clazz);
        // WhereFilter doesn't necessary implement equals, so we need to use the string repr
        assertThat(impl.toString()).isEqualTo(expected);
    }

    private static void opposite(Filter filter, Class<? extends WhereFilter> clazz, String expected) {
        expect(Filter.not(filter), clazz, expected);
        expect(filter.invert(), clazz, expected);
    }
}
