package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import io.deephaven.api.value.Value;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterTest {

    @Test
    void condition() {
        toString(FilterCondition.gt(ColumnName.of("Foo"), Value.of(42L)), "Foo > 42");
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
                FilterAnd.of(Filter.isNotNull(ColumnName.of("Foo")),
                        FilterCondition.gt(ColumnName.of("Foo"), Value.of(42L))),
                "(!isNull(Foo)) && (Foo > 42)");
    }

    @Test
    void ors() {
        toString(
                FilterOr.of(Filter.isNull(ColumnName.of("Foo")),
                        FilterCondition.eq(ColumnName.of("Foo"), Value.of(42L))),
                "(isNull(Foo)) || (Foo == 42)");
    }

    private static void toString(Filter filter, String expected) {
        assertThat(toString(filter)).isEqualTo(expected);
    }

    private static String toString(Filter filter) {
        return Strings.of(filter);
    }
}
