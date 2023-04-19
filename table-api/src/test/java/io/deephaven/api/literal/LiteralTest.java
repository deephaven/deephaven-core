/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.literal;

import io.deephaven.api.Strings;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LiteralTest {

    @Test
    void boolValue() {
        toString(Literal.of(true), "true");
        toString(Literal.of(false), "false");
    }

    @Test
    void intValue() {
        toString(Literal.of(42), "(int)42");
    }

    @Test
    void longValue() {
        toString(Literal.of(42L), "42L");
    }

    @Test
    void stringValue() {
        toString(Literal.of("my string"), "\"my string\"");
        toString(Literal.of("\"my string\""), "\"\\\"my string\\\"\"");
    }

    private static void toString(Literal value, String expected) {
        assertThat(toString(value)).isEqualTo(expected);
    }

    private static String toString(Literal value) {
        return Strings.of(value);
    }
}
