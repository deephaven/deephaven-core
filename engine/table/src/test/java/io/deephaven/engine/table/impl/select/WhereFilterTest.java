/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.value.Literal;
import junit.framework.TestCase;

import static org.assertj.core.api.Assertions.assertThat;

public class WhereFilterTest extends TestCase {

    private static final ColumnName FOO = ColumnName.of("Foo");
    private static final ColumnName BAR = ColumnName.of("Bar");
    private static final Literal V42 = Literal.of(42L);

    public void testIsNull() {
        expect(Filter.isNull(FOO), MatchFilter.class, "Foo in [null]");
    }

    public void testIsNotNull() {
        expect(Filter.isNotNull(FOO), MatchFilter.class, "Foo not in [null]");
    }

    public void testRawString() {
        expect(RawString.of("X * y > foo(Z)"), ConditionFilter.class, "X * y > foo(Z)");
    }

    public void testEq() {
        expect(FilterComparison.eq(FOO, V42), MatchFilter.class, "Foo in [42]");
        expect(FilterComparison.eq(V42, FOO), MatchFilter.class, "Foo in [42]");
        expect(FilterComparison.eq(FOO, BAR), ConditionFilter.class, "Foo == Bar");
    }

    public void testNeq() {
        expect(FilterComparison.neq(FOO, V42), MatchFilter.class, "Foo not in [42]");
        expect(FilterComparison.neq(V42, FOO), MatchFilter.class, "Foo not in [42]");
        expect(FilterComparison.neq(FOO, BAR), ConditionFilter.class, "Foo != Bar");
    }

    public void testGt() {
        expect(FilterComparison.gt(FOO, V42), LongRangeFilter.class,
                "LongRangeFilter(Foo in (42,9223372036854775807])");
        expect(FilterComparison.gt(V42, FOO), LongRangeFilter.class,
                "LongRangeFilter(Foo in [-9223372036854775808,42))");
        expect(FilterComparison.gt(FOO, BAR), ConditionFilter.class, "Foo > Bar");
    }

    public void testGte() {
        expect(FilterComparison.gte(FOO, V42), LongRangeFilter.class,
                "LongRangeFilter(Foo in [42,9223372036854775807])");
        expect(FilterComparison.gte(V42, FOO), LongRangeFilter.class,
                "LongRangeFilter(Foo in [-9223372036854775808,42])");
        expect(FilterComparison.gte(FOO, BAR), ConditionFilter.class, "Foo >= Bar");
    }

    public void testLt() {
        expect(FilterComparison.lt(FOO, V42), LongRangeFilter.class,
                "LongRangeFilter(Foo in [-9223372036854775808,42))");
        expect(FilterComparison.lt(V42, FOO), LongRangeFilter.class,
                "LongRangeFilter(Foo in (42,9223372036854775807])");
        expect(FilterComparison.lt(FOO, BAR), ConditionFilter.class, "Foo < Bar");
    }

    public void testLte() {
        expect(FilterComparison.lte(FOO, V42), LongRangeFilter.class,
                "LongRangeFilter(Foo in [-9223372036854775808,42])");
        expect(FilterComparison.lte(V42, FOO), LongRangeFilter.class,
                "LongRangeFilter(Foo in [42,9223372036854775807])");
        expect(FilterComparison.lte(FOO, BAR), ConditionFilter.class, "Foo <= Bar");
    }

    private static void expect(Filter filter, Class<? extends WhereFilter> clazz, String expected) {
        WhereFilter impl = WhereFilter.of(filter);
        assertThat(impl).isInstanceOf(clazz);
        // WhereFilter doesn't necessary implement equals, so we need to use the string repr
        assertThat(impl.toString()).isEqualTo(expected);
    }
}
