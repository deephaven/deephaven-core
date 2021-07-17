package io.deephaven.api.expression;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ExpressionTest {

    @Test
    void columnNameExpressionToString() {
        assertThat(toString(ColumnName.of("Foo"))).isEqualTo("Foo");
    }

    @Test
    void rawStringExpressionToString() {
        assertThat(toString(RawString.of("Foo + Bar - 42"))).isEqualTo("Foo + Bar - 42");
    }

    private static String toString(Expression expression) {
        return Strings.of(expression);
    }
}
