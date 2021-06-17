package io.deephaven.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class FilterTest {

    @Test
    void columnNameFoo() {
        assertThat(Filter.parse("Foo")).isEqualTo(ColumnName.of("Foo"));
    }

    @Test
    void fooEqBar() {
        assertThat(Filter.parse("Foo==Bar")).isEqualTo(RawString.of("Foo==Bar"));
    }
}
