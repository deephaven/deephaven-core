package io.deephaven.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ExpressionTest {

    @Test
    void columnNameFoo() {
        assertThat(Expression.parse("Foo")).isEqualTo(ColumnName.of("Foo"));
    }

    @Test
    void fooEqBar() {
        assertThat(Expression.parse("Foo=Bar")).isEqualTo(RawString.of("Foo=Bar"));
    }
}
