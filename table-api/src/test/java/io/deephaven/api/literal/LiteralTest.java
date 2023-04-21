/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.literal;

import io.deephaven.api.Strings;
import io.deephaven.api.literal.Literal.Visitor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LiteralTest {

    private static int literalCount() {
        int expected = 0;
        for (java.lang.reflect.Method method : Visitor.class.getMethods()) {
            if ("visit".equals(method.getName()) && method.getParameterCount() == 1) {
                ++expected;
            }
        }
        return expected;
    }

    @Test
    void visitAll() {
        final CountingVisitor visitor = new CountingVisitor();
        visitAll(visitor);
        assertThat(visitor.count).isEqualTo(literalCount());
    }

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

    /**
     * Calls every single visit method of {@code visitor} with a sentinel value or {@code null} object.
     *
     * @param visitor the visitor
     */
    public static void visitAll(Visitor<?> visitor) {
        visitor.visit(false);
        visitor.visit(0);
        visitor.visit(0L);
        visitor.visit((String) null);
    }

    private static class CountingVisitor implements Visitor<CountingVisitor> {
        private int count = 0;

        @Override
        public CountingVisitor visit(boolean literal) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(int literal) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(long literal) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(String literal) {
            ++count;
            return this;
        }
    }
}
