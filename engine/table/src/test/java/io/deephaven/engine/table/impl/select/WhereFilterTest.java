package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterCondition;
import io.deephaven.api.value.Value;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import junit.framework.TestCase;

import static org.assertj.core.api.Assertions.assertThat;

public class WhereFilterTest extends TestCase {

    private static final ColumnName FOO = ColumnName.of("Foo");
    private static final ColumnName BAR = ColumnName.of("Bar");
    private static final Value V42 = Value.of(42L);

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
        expect(FilterCondition.eq(FOO, V42), MatchFilter.class, "Foo in [42]");
        expect(FilterCondition.eq(V42, FOO), MatchFilter.class, "Foo in [42]");
        expect(FilterCondition.eq(FOO, BAR), ConditionFilter.class, "Foo == Bar");
    }

    public void testNeq() {
        expect(FilterCondition.neq(FOO, V42), MatchFilter.class, "Foo not in [42]");
        expect(FilterCondition.neq(V42, FOO), MatchFilter.class, "Foo not in [42]");
        expect(FilterCondition.neq(FOO, BAR), ConditionFilter.class, "Foo != Bar");
    }

    public void testGt() {
        expect(FilterCondition.gt(FOO, V42), LongRangeFilter.class, "LongRangeFilter(Foo in (42,9223372036854775807])");
        expect(FilterCondition.gt(V42, FOO), LongRangeFilter.class,
                "LongRangeFilter(Foo in [-9223372036854775808,42))");
        expect(FilterCondition.gt(FOO, BAR), ConditionFilter.class, "Foo > Bar");
    }

    public void testGte() {
        expect(FilterCondition.gte(FOO, V42), LongRangeFilter.class,
                "LongRangeFilter(Foo in [42,9223372036854775807])");
        expect(FilterCondition.gte(V42, FOO), LongRangeFilter.class,
                "LongRangeFilter(Foo in [-9223372036854775808,42])");
        expect(FilterCondition.gte(FOO, BAR), ConditionFilter.class, "Foo >= Bar");
    }

    public void testLt() {
        expect(FilterCondition.lt(FOO, V42), LongRangeFilter.class,
                "LongRangeFilter(Foo in [-9223372036854775808,42))");
        expect(FilterCondition.lt(V42, FOO), LongRangeFilter.class, "LongRangeFilter(Foo in (42,9223372036854775807])");
        expect(FilterCondition.lt(FOO, BAR), ConditionFilter.class, "Foo < Bar");
    }

    public void testLte() {
        expect(FilterCondition.lte(FOO, V42), LongRangeFilter.class,
                "LongRangeFilter(Foo in [-9223372036854775808,42])");
        expect(FilterCondition.lte(V42, FOO), LongRangeFilter.class,
                "LongRangeFilter(Foo in [42,9223372036854775807])");
        expect(FilterCondition.lte(FOO, BAR), ConditionFilter.class, "Foo <= Bar");
    }

    private static void expect(Filter filter, Class<? extends WhereFilter> clazz, String expected) {
        WhereFilter impl = WhereFilter.of(filter);
        assertThat(impl).isInstanceOf(clazz);
        // WhereFilter doesn't necessary implement equals, so we need to use the string repr
        assertThat(impl.toString()).isEqualTo(expected);
    }
}
