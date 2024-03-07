//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.literal;

import io.deephaven.api.Strings;
import io.deephaven.api.literal.Literal.Visitor;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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
        stringsOf(Literal.of(true), "true");
        stringsOf(Literal.of(false), "false");
    }

    @Test
    void charValue() {
        stringsOf(Literal.of('a'), "'a'");
    }

    @Test
    void byteValue() {
        stringsOf(Literal.of((byte) 42), "(byte)42");
    }

    @Test
    void shortValue() {
        stringsOf(Literal.of((short) 42), "(short)42");
    }

    @Test
    void intValue() {
        stringsOf(Literal.of(42), "(int)42");
    }

    @Test
    void longValue() {
        stringsOf(Literal.of(42L), "42L");
    }

    @Test
    void floatValue() {
        stringsOf(Literal.of(42.0f), "42.0f");
    }

    @Test
    void doubleValue() {
        stringsOf(Literal.of(42.0), "42.0");
    }

    @Test
    void stringValue() {
        stringsOf(Literal.of("my string"), "\"my string\"");
        stringsOf(Literal.of("\"my string\""), "\"\\\"my string\\\"\"");
    }

    @Test
    void examplesStringsOf() {
        for (Literal literal : Examples.of()) {
            stringsOf(literal);
        }
    }

    private static void stringsOf(Literal value, String expected) {
        assertThat(stringsOf(value)).isEqualTo(expected);
    }

    private static String stringsOf(Literal value) {
        return Strings.of(value);
    }

    /**
     * Calls every single visit method of {@code visitor} with a sentinel value or {@code null} object.
     *
     * @param visitor the visitor
     */
    public static void visitAll(Visitor<?> visitor) {
        visitor.visit(false);
        visitor.visit((char) 0);
        visitor.visit((byte) 0);
        visitor.visit((short) 0);
        visitor.visit(0);
        visitor.visit(0L);
        visitor.visit(0.0f);
        visitor.visit(0.0);
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
        public CountingVisitor visit(char literal) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(byte literal) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(short literal) {
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
        public CountingVisitor visit(float literal) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(double literal) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(String literal) {
            ++count;
            return this;
        }
    }

    public static class Examples implements Literal.Visitor<Void> {

        public static List<Literal> of() {
            Examples visitor = new Examples();
            visitAll(visitor);
            final List<Literal> out = new ArrayList<>(visitor.out);
            // Let's supplement all of the produced values w/ their Object toStrings
            for (Literal literal : visitor.out) {
                if (!(literal instanceof LiteralString)) {
                    out.add(Literal.of(literal.walk(ToObject.INSTANCE).toString()));
                }
            }
            return out;
        }

        private final List<Literal> out = new ArrayList<>();

        @Override
        public Void visit(boolean literal) {
            out.add(Literal.of(false));
            out.add(Literal.of(true));
            return null;
        }

        @Override
        public Void visit(char literal) {
            out.add(Literal.of('a'));
            out.add(Literal.of('A'));
            out.add(Literal.of('☺'));

            out.add(Literal.of((char) QueryConstants.NULL_BYTE));
            out.add(Literal.of((char) QueryConstants.MIN_BYTE));
            out.add(Literal.of((char) QueryConstants.MAX_BYTE));

            out.add(Literal.of(Character.MIN_LOW_SURROGATE));
            out.add(Literal.of(Character.MIN_HIGH_SURROGATE));
            out.add(Literal.of(Character.MAX_LOW_SURROGATE));
            out.add(Literal.of(Character.MAX_HIGH_SURROGATE));
            out.add(Literal.of(QueryConstants.MIN_CHAR));
            out.add(Literal.of(QueryConstants.MAX_CHAR));
            return null;
        }

        @Override
        public Void visit(byte literal) {
            out.add(Literal.of((byte) 0));
            out.add(Literal.of((byte) -1));
            out.add(Literal.of((byte) 1));
            out.add(Literal.of(QueryConstants.MIN_BYTE));
            out.add(Literal.of(QueryConstants.MAX_BYTE));
            return null;
        }

        @Override
        public Void visit(short literal) {
            out.add(Literal.of((short) 0));
            out.add(Literal.of((short) -1));
            out.add(Literal.of((short) 1));

            out.add(Literal.of((short) QueryConstants.NULL_CHAR));
            out.add(Literal.of((short) QueryConstants.MIN_CHAR));
            out.add(Literal.of((short) QueryConstants.MAX_CHAR));

            out.add(Literal.of((short) QueryConstants.NULL_BYTE));
            out.add(Literal.of((short) QueryConstants.MIN_BYTE));
            out.add(Literal.of((short) QueryConstants.MAX_BYTE));

            out.add(Literal.of(QueryConstants.MIN_SHORT));
            out.add(Literal.of(QueryConstants.MAX_SHORT));
            return null;
        }

        @Override
        public Void visit(int literal) {
            out.add(Literal.of(0));
            out.add(Literal.of(-1));
            out.add(Literal.of(1));

            out.add(Literal.of((int) QueryConstants.NULL_CHAR));
            out.add(Literal.of((int) QueryConstants.MIN_CHAR));
            out.add(Literal.of((int) QueryConstants.MAX_CHAR));

            out.add(Literal.of((int) QueryConstants.NULL_BYTE));
            out.add(Literal.of((int) QueryConstants.MIN_BYTE));
            out.add(Literal.of((int) QueryConstants.MAX_BYTE));

            out.add(Literal.of((int) QueryConstants.NULL_SHORT));
            out.add(Literal.of((int) QueryConstants.MIN_SHORT));
            out.add(Literal.of((int) QueryConstants.MAX_SHORT));

            out.add(Literal.of(QueryConstants.MIN_INT));
            out.add(Literal.of(QueryConstants.MAX_INT));
            return null;
        }

        @Override
        public Void visit(long literal) {
            out.add(Literal.of(0L));
            out.add(Literal.of(-1L));
            out.add(Literal.of(1L));

            out.add(Literal.of((long) QueryConstants.NULL_CHAR));
            out.add(Literal.of((long) QueryConstants.MIN_CHAR));
            out.add(Literal.of((long) QueryConstants.MAX_CHAR));

            out.add(Literal.of((long) QueryConstants.NULL_BYTE));
            out.add(Literal.of((long) QueryConstants.MIN_BYTE));
            out.add(Literal.of((long) QueryConstants.MAX_BYTE));

            out.add(Literal.of((long) QueryConstants.NULL_SHORT));
            out.add(Literal.of((long) QueryConstants.MIN_SHORT));
            out.add(Literal.of((long) QueryConstants.MAX_SHORT));

            out.add(Literal.of((long) QueryConstants.NULL_INT));
            out.add(Literal.of((long) QueryConstants.MIN_INT));
            out.add(Literal.of((long) QueryConstants.MAX_INT));

            out.add(Literal.of(QueryConstants.MIN_LONG));
            out.add(Literal.of(QueryConstants.MAX_LONG));
            return null;
        }

        @Override
        public Void visit(float literal) {
            out.add(Literal.of(0.0f));
            out.add(Literal.of(-0.0f));
            out.add(Literal.of(-1.0f));
            out.add(Literal.of(1.0f));
            out.add(Literal.of(Float.NaN));
            out.add(Literal.of(Float.MIN_VALUE));
            out.add(Literal.of(Float.MIN_NORMAL));
            out.add(Literal.of(-Float.MIN_VALUE));
            out.add(Literal.of(-Float.MIN_NORMAL));
            out.add(Literal.of(Float.MAX_VALUE));
            out.add(Literal.of(QueryConstants.MIN_FLOAT));
            out.add(Literal.of(QueryConstants.MAX_FLOAT));
            return null;
        }

        @Override
        public Void visit(double literal) {
            out.add(Literal.of(0.0));
            out.add(Literal.of(-0.0));
            out.add(Literal.of(-1.0));
            out.add(Literal.of(1.0));
            out.add(Literal.of(Double.NaN));
            out.add(Literal.of(Double.MIN_VALUE));
            out.add(Literal.of(Double.MIN_NORMAL));
            out.add(Literal.of(-Double.MIN_VALUE));
            out.add(Literal.of(-Double.MIN_NORMAL));
            out.add(Literal.of(Double.MAX_VALUE));

            out.add(Literal.of((double) Float.MIN_VALUE));
            out.add(Literal.of((double) Float.MIN_NORMAL));
            out.add(Literal.of(-(double) Float.MIN_VALUE));
            out.add(Literal.of(-(double) Float.MIN_NORMAL));
            out.add(Literal.of((double) Float.MAX_VALUE));
            out.add(Literal.of((double) QueryConstants.NULL_FLOAT));
            out.add(Literal.of((double) QueryConstants.MIN_FLOAT));
            out.add(Literal.of((double) QueryConstants.MAX_FLOAT));

            out.add(Literal.of(QueryConstants.MIN_DOUBLE));
            out.add(Literal.of(QueryConstants.MAX_DOUBLE));
            return null;
        }

        @Override
        public Void visit(String literal) {
            out.add(Literal.of(""));
            out.add(Literal.of("null"));
            out.add(Literal.of("None"));
            out.add(Literal.of("Foo"));
            out.add(Literal.of("BAR"));
            out.add(Literal.of("foo bar baz"));
            out.add(Literal.of("\n"));
            return null;
        }
    }

    private enum ToObject implements Literal.Visitor<Object> {
        INSTANCE;

        @Override
        public Object visit(boolean literal) {
            return literal;
        }

        @Override
        public Object visit(char literal) {
            return literal;
        }

        @Override
        public Object visit(byte literal) {
            return literal;
        }

        @Override
        public Object visit(short literal) {
            return literal;
        }

        @Override
        public Object visit(int literal) {
            return literal;
        }

        @Override
        public Object visit(long literal) {
            return literal;
        }

        @Override
        public Object visit(float literal) {
            return literal;
        }

        @Override
        public Object visit(double literal) {
            return literal;
        }

        @Override
        public Object visit(String literal) {
            return literal;
        }
    }
}
