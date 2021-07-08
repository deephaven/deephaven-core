package io.deephaven.api.value;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ValueTest {

    @Test
    void columnName() {
        toString(ColumnName.of("Foo"), "Foo");
    }

    @Test
    void longValue() {
        toString(Value.of(42L), "42");
    }

    private static void toString(Value value, String expected) {
        assertThat(toString(value)).isEqualTo(expected);
    }

    private static String toString(Value value) {
        return Strings.of(value);
    }
}
