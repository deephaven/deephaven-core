package io.deephaven.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class JoinMatchTest {

    @Test
    void columnNameFoo() {
        assertThat(JoinMatch.parse("Foo")).isEqualTo(ColumnName.of("Foo"));
    }

    @Test
    void fooEqBar() {
        assertThat(JoinMatch.parse("Foo==Bar"))
            .isEqualTo(ColumnMatch.of(ColumnName.of("Foo"), ColumnName.of("Bar")));
    }
}
