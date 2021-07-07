package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterTest {

    @Test
    void columnNameFilterToString() {
        assertThat(toString(ColumnName.of("Foo"))).isEqualTo("Foo");
    }

    @Test
    void rawStringFilterToString() {
        assertThat(toString(RawString.of("Foo - 1 > Bar"))).isEqualTo("Foo - 1 > Bar");
    }

    @Test
    void filterMatchFilterToString() {
        assertThat(toString(FilterMatch.of(ColumnName.of("Foo"), ColumnName.of("Bar"))))
            .isEqualTo("Foo==Bar");
    }

    private static String toString(Filter filter) {
        return Strings.of(filter);
    }
}
