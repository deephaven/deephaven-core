package io.deephaven.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class JoinAdditionTest {

    @Test
    void columnNameFoo() {
        assertThat(JoinAddition.parse("Foo")).isEqualTo(ColumnName.of("Foo"));
    }

    @Test
    void fooEqBar() {
        assertThat(JoinAddition.parse("Foo=Bar"))
            .isEqualTo(ColumnAssignment.of(ColumnName.of("Foo"), ColumnName.of("Bar")));
    }
}
