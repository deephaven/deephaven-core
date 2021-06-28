package io.deephaven.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ExpressionTest {

    @Test
    void foo() {
        assertThat(Expression.parse("Foo")).isEqualTo(RawString.of("Foo"));
    }

    @Test
    void fooEqBar() {
        assertThat(Expression.parse("Foo=Bar")).isEqualTo(RawString.of("Foo=Bar"));
    }
}
