//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.expression;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Strings;
import io.deephaven.api.expression.Expression.Visitor;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterTest;
import io.deephaven.api.literal.Literal;
import io.deephaven.api.literal.LiteralTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.api.Strings.of;
import static io.deephaven.api.filter.Filter.and;
import static io.deephaven.api.filter.Filter.isNotNull;
import static io.deephaven.api.filter.Filter.isNull;
import static io.deephaven.api.filter.Filter.ofFalse;
import static io.deephaven.api.filter.Filter.ofTrue;
import static io.deephaven.api.filter.Filter.or;
import static io.deephaven.api.filter.FilterComparison.eq;
import static io.deephaven.api.filter.FilterComparison.gt;
import static io.deephaven.api.filter.FilterComparison.neq;
import static org.assertj.core.api.Assertions.assertThat;

public class ExpressionTest {

    public static final ColumnName FOO = ColumnName.of("Foo");
    public static final ColumnName BAR = ColumnName.of("Bar");
    public static final ColumnName BAZ = ColumnName.of("Baz");

    private static int expressionCount() {
        int expected = 0;
        for (java.lang.reflect.Method method : Visitor.class.getMethods()) {
            if ("visit".equals(method.getName()) && method.getParameterCount() == 1
                    && Expression.class.isAssignableFrom(method.getParameterTypes()[0])) {
                ++expected;
            }
        }
        return expected;
    }

    @Test
    void visitAll() {
        final CountingVisitor visitor = new CountingVisitor();
        visitAll(visitor);
        assertThat(visitor.count).isEqualTo(expressionCount());
    }

    @Test
    void columnName() {
        stringsOf(FOO, "Foo");
    }

    @Test
    void filter() {
        stringsOf(or(gt(FOO, BAR), gt(FOO, BAZ), and(isNull(FOO), isNotNull(BAR), isNotNull(BAZ))),
                "(Foo > Bar) || (Foo > Baz) || (isNull(Foo) && !isNull(Bar) && !isNull(Baz))");
    }

    @Test
    void expressionFunction() {
        stringsOf(Function.of("plus", FOO, BAR), "plus(Foo, Bar)");
        stringsOf(Function.of("plus", FOO, Function.of("minus", BAR, BAZ)), "plus(Foo, minus(Bar, Baz))");
    }

    @Test
    void expressionFunctionThatTakesFilters() {
        stringsOf(
                Function.of("some_func", gt(FOO, BAR), BAZ, ofTrue(), ofFalse(),
                        and(isNull(FOO), isNotNull(BAR), or(eq(FOO, BAR), neq(FOO, BAZ)))),
                "some_func(Foo > Bar, Baz, true, false, isNull(Foo) && !isNull(Bar) && ((Foo == Bar) || (Foo != Baz)))");
    }

    @Test
    void expressionMethod() {
        stringsOf(Method.of(FOO, "myMethod", BAR), "Foo.myMethod(Bar)");
    }

    @Test
    void literals() {
        stringsOf(Literal.of(true), "true");
        stringsOf(Literal.of(false), "false");
        stringsOf(Literal.of(42), "(int)42");
        stringsOf(Literal.of(42L), "42L");
        stringsOf(Literal.of("foo bar"), "\"foo bar\"");
        stringsOf(Literal.of("\"foo bar\""), "\"\\\"foo bar\\\"\"");
    }

    @Test
    void rawString() {
        stringsOf(RawString.of("Foo + Bar - 42"), "Foo + Bar - 42");
    }

    @Test
    void examplesStringsOf() {
        for (Expression expression : Examples.of()) {
            Strings.of(expression);
        }
    }

    private static void stringsOf(Expression expression, String expected) {
        assertThat(of(expression)).isEqualTo(expected);
        assertThat(expression.walk(SpecificMethod.INSTANCE)).isEqualTo(expected);
    }

    private enum SpecificMethod implements Expression.Visitor<String> {
        INSTANCE;

        @Override
        public String visit(Literal literal) {
            return of(literal);
        }

        @Override
        public String visit(ColumnName columnName) {
            return of(columnName);
        }

        @Override
        public String visit(Filter filter) {
            return of(filter);
        }

        @Override
        public String visit(Function function) {
            return of(function);
        }

        @Override
        public String visit(Method method) {
            return of(method);
        }

        @Override
        public String visit(RawString rawString) {
            return of(rawString);
        }
    }

    /**
     * Calls every single visit method of {@code visitor} with a {@code null} object.
     *
     * @param visitor the visitor
     */
    public static void visitAll(Visitor<?> visitor) {
        visitor.visit((Literal) null);
        visitor.visit((ColumnName) null);
        visitor.visit((Filter) null);
        visitor.visit((Function) null);
        visitor.visit((Method) null);
        visitor.visit((RawString) null);
    }

    private static class CountingVisitor implements Expression.Visitor<CountingVisitor> {
        private int count = 0;

        @Override
        public CountingVisitor visit(Literal literal) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(ColumnName columnName) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(Filter filter) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(Function function) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(Method method) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(RawString rawString) {
            ++count;
            return this;
        }
    }

    public static class Examples implements Expression.Visitor<Void> {

        public static List<Expression> of() {
            final Examples visitor = new Examples();
            visitAll(visitor);
            return visitor.out;
        }

        private final List<Expression> out = new ArrayList<>();

        @Override
        public Void visit(Literal literal) {
            out.addAll(LiteralTest.Examples.of());
            return null;
        }

        @Override
        public Void visit(ColumnName columnName) {
            out.add(FOO);
            out.add(BAR);
            out.add(BAZ);
            return null;
        }

        @Override
        public Void visit(Filter filter) {
            out.addAll(FilterTest.Examples.of());
            return null;
        }

        @Override
        public Void visit(Function function) {
            out.add(Function.of("my_function", FOO));
            return null;
        }

        @Override
        public Void visit(Method method) {
            out.add(Method.of(FOO, "whats", BAR));
            return null;
        }

        @Override
        public Void visit(RawString rawString) {
            out.add(RawString.of("Foo + Bar"));
            out.add(RawString.of("Foo > Bar + 42"));
            out.add(RawString.of("!Foo - what_isTHIS(Bar)"));
            out.add(RawString.of("blerg9"));
            return null;
        }
    }
}
