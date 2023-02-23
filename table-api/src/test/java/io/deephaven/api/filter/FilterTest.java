/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import io.deephaven.api.value.Literal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterTest {

    @Test
    void condition() {
        toString(FilterComparison.gt(ColumnName.of("Foo"), Literal.of(42L)), "(Foo) > (42L)");
    }

    @Test
    void isNull() {
        toString(Filter.isNull(ColumnName.of("Foo")), "isNull(Foo)");
    }

    @Test
    void isNotNull() {
        toString(Filter.isNotNull(ColumnName.of("Foo")), "!isNull(Foo)");
    }

    @Test
    void not() {
        toString(Filter.not(Filter.isNull(ColumnName.of("Foo"))), "!(isNull(Foo))");
    }

    @Test
    void ands() {
        toString(
                Filter.and(Filter.isNotNull(ColumnName.of("Foo")),
                        FilterComparison.gt(ColumnName.of("Foo"), Literal.of(42L))),
                "(!isNull(Foo)) && ((Foo) > (42L))");
    }

    @Test
    void ors() {
        toString(
                Filter.or(Filter.isNull(ColumnName.of("Foo")),
                        FilterComparison.eq(ColumnName.of("Foo"), Literal.of(42L))),
                "(isNull(Foo)) || ((Foo) == (42L))");
    }

    private static void toString(Filter filter, String expected) {
        assertThat(toString(filter)).isEqualTo(expected);
    }

    private static String toString(Filter filter) {
        return Strings.of(filter);
    }
}
