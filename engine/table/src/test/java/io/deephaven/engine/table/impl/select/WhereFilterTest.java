/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.filter.FilterPattern.Mode;
import io.deephaven.api.filter.FilterQuick;
import io.deephaven.api.literal.Literal;
import junit.framework.TestCase;

import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class WhereFilterTest extends TestCase {

    private static final ColumnName FOO = ColumnName.of("Foo");
    private static final ColumnName BAR = ColumnName.of("Bar");
    private static final ColumnName BAZ = ColumnName.of("Baz");
    private static final Literal V42 = Literal.of(42L);

    public void testFooIsTrue() {
        regular(Filter.isTrue(FOO), MatchFilter.class, "Foo in [true]");
        inverse(Filter.isTrue(FOO), MatchFilter.class, "Foo not in [true]");
    }

    public void testFooIsFalse() {
        regular(Filter.isFalse(FOO), MatchFilter.class, "Foo in [false]");
        inverse(Filter.isFalse(FOO), MatchFilter.class, "Foo not in [false]");
    }

    public void testFooIsNull() {
        regular(Filter.isNull(FOO), MatchFilter.class, "Foo in [null]");
        inverse(Filter.isNull(FOO), MatchFilter.class, "Foo not in [null]");
    }

    public void testFooIsNotNull() {
        regular(Filter.isNotNull(FOO), MatchFilter.class, "Foo not in [null]");
        inverse(Filter.isNotNull(FOO), MatchFilter.class, "Foo in [null]");
    }

    public void testFooAndBar() {
        regular(Filter.and(Filter.isTrue(FOO), Filter.isTrue(BAR)), ConjunctiveFilter.class,
                "ConjunctiveFilter([Foo in [true], Bar in [true]])");
        inverse(Filter.and(Filter.isTrue(FOO), Filter.isTrue(BAR)), DisjunctiveFilter.class,
                "DisjunctiveFilter([Foo not in [true], Bar not in [true]])");
    }

    public void testFooOrBar() {
        regular(Filter.or(Filter.isTrue(FOO), Filter.isTrue(BAR)), DisjunctiveFilter.class,
                "DisjunctiveFilter([Foo in [true], Bar in [true]])");
        inverse(Filter.or(Filter.isTrue(FOO), Filter.isTrue(BAR)), ConjunctiveFilter.class,
                "ConjunctiveFilter([Foo not in [true], Bar not in [true]])");
    }

    public void testRawString() {
        regular(RawString.of("X * y > foo(Z)"), ConditionFilter.class, "X * y > foo(Z)");
        inverse(RawString.of("X * y > foo(Z)"), ConditionFilter.class, "!(X * y > foo(Z))");
    }

    public void testEq() {
        regular(FilterComparison.eq(FOO, V42), MatchFilter.class, "Foo in [42]");
        regular(FilterComparison.eq(V42, FOO), MatchFilter.class, "Foo in [42]");
        regular(FilterComparison.eq(FOO, BAR), ConditionFilter.class, "Foo == Bar");

        inverse(FilterComparison.eq(FOO, V42), MatchFilter.class, "Foo not in [42]");
        inverse(FilterComparison.eq(V42, FOO), MatchFilter.class, "Foo not in [42]");
        inverse(FilterComparison.eq(FOO, BAR), ConditionFilter.class, "Foo != Bar");
    }

    public void testNeq() {
        regular(FilterComparison.neq(FOO, V42), MatchFilter.class, "Foo not in [42]");
        regular(FilterComparison.neq(V42, FOO), MatchFilter.class, "Foo not in [42]");
        regular(FilterComparison.neq(FOO, BAR), ConditionFilter.class, "Foo != Bar");

        inverse(FilterComparison.neq(FOO, V42), MatchFilter.class, "Foo in [42]");
        inverse(FilterComparison.neq(V42, FOO), MatchFilter.class, "Foo in [42]");
        inverse(FilterComparison.neq(FOO, BAR), ConditionFilter.class, "Foo == Bar");
    }

    public void testGt() {
        regular(FilterComparison.gt(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        regular(FilterComparison.gt(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        regular(FilterComparison.gt(FOO, BAR), ConditionFilter.class, "Foo > Bar");

        inverse(FilterComparison.gt(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        inverse(FilterComparison.gt(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        inverse(FilterComparison.gt(FOO, BAR), ConditionFilter.class, "Foo <= Bar");
    }

    public void testGte() {
        regular(FilterComparison.geq(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        regular(FilterComparison.geq(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        regular(FilterComparison.geq(FOO, BAR), ConditionFilter.class, "Foo >= Bar");

        inverse(FilterComparison.geq(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        inverse(FilterComparison.geq(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        inverse(FilterComparison.geq(FOO, BAR), ConditionFilter.class, "Foo < Bar");
    }

    public void testLt() {
        regular(FilterComparison.lt(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        regular(FilterComparison.lt(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        regular(FilterComparison.lt(FOO, BAR), ConditionFilter.class, "Foo < Bar");

        inverse(FilterComparison.lt(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        inverse(FilterComparison.lt(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        inverse(FilterComparison.lt(FOO, BAR), ConditionFilter.class, "Foo >= Bar");
    }

    public void testLte() {
        regular(FilterComparison.leq(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than or equal to 42)");
        regular(FilterComparison.leq(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than or equal to 42)");
        regular(FilterComparison.leq(FOO, BAR), ConditionFilter.class, "Foo <= Bar");

        inverse(FilterComparison.leq(FOO, V42), RangeConditionFilter.class,
                "RangeConditionFilter(Foo greater than 42)");
        inverse(FilterComparison.leq(V42, FOO), RangeConditionFilter.class,
                "RangeConditionFilter(Foo less than 42)");
        inverse(FilterComparison.leq(FOO, BAR), ConditionFilter.class, "Foo > Bar");
    }

    public void testFunction() {
        regular(Function.of("someMethod"), ConditionFilter.class, "someMethod()");
        inverse(Function.of("someMethod"), ConditionFilter.class, "!someMethod()");

        regular(Function.of("someMethod", FOO), ConditionFilter.class, "someMethod(Foo)");
        inverse(Function.of("someMethod", FOO), ConditionFilter.class, "!someMethod(Foo)");

        regular(Function.of("someMethod", FOO, BAR), ConditionFilter.class, "someMethod(Foo, Bar)");
        inverse(Function.of("someMethod", FOO, BAR), ConditionFilter.class, "!someMethod(Foo, Bar)");
    }

    public void testFunctionIsNull() {
        regular(Filter.isNull(Function.of("someMethod", FOO, BAR)), ConditionFilter.class,
                "isNull(someMethod(Foo, Bar))");
        inverse(Filter.isNull(Function.of("someMethod", FOO, BAR)), ConditionFilter.class,
                "!isNull(someMethod(Foo, Bar))");
    }

    public void testFunctionIsNotNull() {
        regular(Filter.isNotNull(Function.of("someMethod", FOO, BAR)), ConditionFilter.class,
                "!isNull(someMethod(Foo, Bar))");
        inverse(Filter.isNotNull(Function.of("someMethod", FOO, BAR)), ConditionFilter.class,
                "isNull(someMethod(Foo, Bar))");
    }

    public void testMethod() {
        regular(Method.of(BAZ, "someMethod"), ConditionFilter.class, "Baz.someMethod()");
        inverse(Method.of(BAZ, "someMethod"), ConditionFilter.class, "!Baz.someMethod()");

        regular(Method.of(BAZ, "someMethod", FOO), ConditionFilter.class, "Baz.someMethod(Foo)");
        inverse(Method.of(BAZ, "someMethod", FOO), ConditionFilter.class, "!Baz.someMethod(Foo)");

        regular(Method.of(BAZ, "someMethod", FOO, BAR), ConditionFilter.class, "Baz.someMethod(Foo, Bar)");
        inverse(Method.of(BAZ, "someMethod", FOO, BAR), ConditionFilter.class, "!Baz.someMethod(Foo, Bar)");
    }

    public void testLiteralIsTrue() {
        regular(Filter.isTrue(Literal.of(42)), ConditionFilter.class, "(int)42 == true");
        inverse(Filter.isTrue(Literal.of(42)), ConditionFilter.class, "(int)42 != true");
    }

    public void testLiteralIsFalse() {
        regular(Filter.isFalse(Literal.of(42)), ConditionFilter.class, "(int)42 == false");
        inverse(Filter.isFalse(Literal.of(42)), ConditionFilter.class, "(int)42 != false");
    }

    public void testLiteralIsNull() {
        regular(Filter.isNull(Literal.of(42)), ConditionFilter.class, "isNull((int)42)");
        inverse(Filter.isNull(Literal.of(42)), ConditionFilter.class, "!isNull((int)42)");
    }

    public void testLiteralIsNotNull() {
        regular(Filter.isNotNull(Literal.of(42)), ConditionFilter.class, "!isNull((int)42)");
        inverse(Filter.isNotNull(Literal.of(42)), ConditionFilter.class, "isNull((int)42)");
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

    public void testAnd() {
        final Filter filter = Filter.and(Filter.isNull(FOO), Filter.isNotNull(BAR));
        regular(filter, ConjunctiveFilter.class, "ConjunctiveFilter([Foo in [null], Bar not in [null]])");
        inverse(filter, DisjunctiveFilter.class, "DisjunctiveFilter([Foo not in [null], Bar in [null]])");
    }

    public void testOr() {
        final Filter filter = Filter.or(Filter.isNull(FOO), Filter.isNotNull(BAR));
        regular(filter, DisjunctiveFilter.class, "DisjunctiveFilter([Foo in [null], Bar not in [null]])");
        inverse(filter, ConjunctiveFilter.class, "ConjunctiveFilter([Foo not in [null], Bar in [null]])");
    }

    public void testPattern() {
        final String str = "FilterPattern(ColumnName(Foo), myregex, 0, FIND, false)";
        final FilterPattern pattern = FilterPattern.of(FOO, Pattern.compile("myregex"), Mode.FIND, false);
        regular(pattern, WhereFilterPatternImpl.class, str);
        regularInverse(pattern, WhereFilterPatternImpl.class, str);
    }

    public void testQuick() {
        final String str = "FilterQuick(ColumnName(Foo), bar)";
        final FilterQuick quick = FilterQuick.of(FOO, "bar");
        regular(quick, WhereFilterQuickImpl.class, str);
        regularInverse(quick, WhereFilterQuickImpl.class, str);
    }

    public void testInSingle() {
        final FilterIn in = FilterIn.of(FOO, Literal.of(40));
        regular(in, MatchFilter.class, "Foo in [40]");
        inverse(in, MatchFilter.class, "Foo not in [40]");
    }

    public void testInSingleString() {
        final FilterIn in = FilterIn.of(FOO, Literal.of("mystr"));
        regular(in, MatchFilter.class, "Foo in [mystr]");
        inverse(in, MatchFilter.class, "Foo not in [mystr]");
    }

    public void testInLiterals() {
        final FilterIn in = FilterIn.of(FOO, Literal.of(40), Literal.of(42));
        regular(in, MatchFilter.class, "Foo in [40, 42]");
        inverse(in, MatchFilter.class, "Foo not in [40, 42]");
    }

    public void testInLiteralsDifferentTypes() {
        final FilterIn in = FilterIn.of(FOO, Literal.of(40), Literal.of("mystr"));
        regular(in, MatchFilter.class, "Foo in [40, mystr]");
        inverse(in, MatchFilter.class, "Foo not in [40, mystr]");
    }

    public void testInSingleNotLiteral() {
        final FilterIn in = FilterIn.of(FOO, BAR);
        regular(in, ConditionFilter.class, "Foo == Bar");
        inverse(in, ConditionFilter.class, "Foo != Bar");
    }


    public void testInNotAllLiterals() {
        final FilterIn in = FilterIn.of(FOO, Literal.of(40), BAR);
        regular(in, DisjunctiveFilter.class, "DisjunctiveFilter([Foo in [40], Foo == Bar])");
        inverse(in, ConjunctiveFilter.class, "ConjunctiveFilter([Foo not in [40], Foo != Bar])");
    }

    public void testRaw() {
        final RawString filter = RawString.of("some_crazy_thing(x, y, z)");
        regular(filter, ConditionFilter.class, "some_crazy_thing(x, y, z)");
        inverse(filter, ConditionFilter.class, "!(some_crazy_thing(x, y, z))");
    }

    private static <T extends WhereFilter> T regular(Filter f, Class<T> clazz, String expected) {
        WhereFilter filter = WhereFilter.of(f);
        assertThat(filter).isInstanceOf(clazz);
        // WhereFilter doesn't necessary implement equals, so we need to use the string repr
        assertThat(filter.toString()).isEqualTo(expected);
        {
            // Ensure doubly nested negations produce the same results
            final FilterNot<FilterNot<Filter>> doubleNot = Filter.not(Filter.not(f));
            final WhereFilter filter2 = WhereFilter.of(doubleNot);
            assertThat(filter2).isInstanceOf(clazz);
            assertThat(filter2.toString()).isEqualTo(expected);
        }
        return clazz.cast(filter);
    }

    private static <T extends WhereFilter> T inverse(Filter f, Class<T> clazz, String expected) {
        regular(Filter.not(f), clazz, expected);
        return regular(f.invert(), clazz, expected);
    }

    private static void regularInverse(Filter f, Class<? extends WhereFilter> clazz, String expected) {
        final WhereFilterInvertedImpl filter = inverse(f, WhereFilterInvertedImpl.class, "not(" + expected + ")");
        final WhereFilter inner = filter.filter();
        assertThat(inner).isInstanceOf(clazz);
    }
}
