/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter.Visitor;
import io.deephaven.api.literal.Literal;
import org.junit.jupiter.api.Test;

import static io.deephaven.api.Strings.of;
import static io.deephaven.api.filter.Filter.and;
import static io.deephaven.api.filter.Filter.isFalse;
import static io.deephaven.api.filter.Filter.isNotNull;
import static io.deephaven.api.filter.Filter.isNull;
import static io.deephaven.api.filter.Filter.isTrue;
import static io.deephaven.api.filter.Filter.not;
import static io.deephaven.api.filter.Filter.ofFalse;
import static io.deephaven.api.filter.Filter.ofTrue;
import static io.deephaven.api.filter.Filter.or;
import static io.deephaven.api.filter.FilterComparison.eq;
import static io.deephaven.api.filter.FilterComparison.gt;
import static io.deephaven.api.filter.FilterComparison.neq;
import static org.assertj.core.api.Assertions.assertThat;

public class FilterTest {

    private static final ColumnName FOO = ColumnName.of("Foo");
    private static final ColumnName BAR = ColumnName.of("Bar");
    private static final ColumnName BAZ = ColumnName.of("Baz");

    private static final Literal L42 = Literal.of(42L);

    private static int filterCount() {
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
        assertThat(visitor.count).isEqualTo(filterCount());
    }

    @Test
    void filterIsNull() {
        toString(isNull(FOO), "isNull(Foo)");
        toString(not(isNull(FOO)), "!isNull(Foo)");
    }

    @Test
    void filterIsNotNull() {
        toString(isNotNull(FOO), "!isNull(Foo)");
        toString(not(isNotNull(FOO)), "isNull(Foo)");
    }

    @Test
    void filterNot() {
        toString(not(isNull(FOO)), "!isNull(Foo)");
    }

    @Test
    void filterAnd() {
        toString(and(isNotNull(FOO), isNotNull(BAR)), "!isNull(Foo) && !isNull(Bar)");
        toString(not(and(isNotNull(FOO), isNotNull(BAR))), "isNull(Foo) || isNull(Bar)");
    }

    @Test
    void filterOr() {
        toString(or(isNull(FOO), gt(FOO, BAR)), "isNull(Foo) || (Foo > Bar)");
        toString(not(or(isNull(FOO), gt(FOO, BAR))), "!isNull(Foo) && (Foo <= Bar)");
    }

    @Test
    void filterOfTrue() {
        toString(ofTrue(), "true");
        toString(not(ofTrue()), "false");
    }

    @Test
    void filterOfFalse() {
        toString(ofFalse(), "false");
        toString(not(ofFalse()), "true");
    }

    @Test
    void filterIsTrue() {
        toString(isTrue(FOO), "Foo == true");
        toString(not(isTrue(FOO)), "Foo != true");
    }

    @Test
    void filterIsFalse() {
        toString(isFalse(FOO), "Foo == false");
        toString(not(isFalse(FOO)), "Foo != false");
    }

    @Test
    void filterEqPrecedence() {
        toString(eq(or(isTrue(FOO), eq(BAR, BAZ)), and(isTrue(FOO), neq(BAR, BAZ))),
                "((Foo == true) || (Bar == Baz)) == ((Foo == true) && (Bar != Baz))");
        toString(not(eq(or(isTrue(FOO), eq(BAR, BAZ)), and(isTrue(FOO), neq(BAR, BAZ)))),
                "((Foo == true) || (Bar == Baz)) != ((Foo == true) && (Bar != Baz))");
    }

    @Test
    void filterFunction() {
        toString(Function.of("MyFunction1"), "MyFunction1()");
        toString(Function.of("MyFunction2", FOO), "MyFunction2(Foo)");
        toString(Function.of("MyFunction3", FOO, BAR), "MyFunction3(Foo, Bar)");

        toString(not(Function.of("MyFunction1")), "!MyFunction1()");
        toString(not(Function.of("MyFunction2", FOO)), "!MyFunction2(Foo)");
        toString(not(Function.of("MyFunction3", FOO, BAR)), "!MyFunction3(Foo, Bar)");
    }

    @Test
    void filterMethod() {
        toString(Method.of(FOO, "MyFunction1"), "Foo.MyFunction1()");
        toString(Method.of(FOO, "MyFunction2", BAR), "Foo.MyFunction2(Bar)");
        toString(Method.of(FOO, "MyFunction3", BAR, BAZ), "Foo.MyFunction3(Bar, Baz)");

        toString(not(Method.of(FOO, "MyFunction1")), "!Foo.MyFunction1()");
        toString(not(Method.of(FOO, "MyFunction2", BAR)), "!Foo.MyFunction2(Bar)");
        toString(not(Method.of(FOO, "MyFunction3", BAR, BAZ)), "!Foo.MyFunction3(Bar, Baz)");
    }

    @Test
    void filterRawString() {
        toString(RawString.of("this is a raw string"), "this is a raw string");
        toString(Filter.not(RawString.of("this is a raw string")), "!(this is a raw string)");
    }

    private static void toString(Filter filter, String expected) {
        assertThat(of(filter)).isEqualTo(expected);
        assertThat(filter.walk(FilterSpecificString.INSTANCE)).isEqualTo(expected);
    }

    /**
     * Calls every single visit method of {@code visitor} with a sentinel value or {@code null} object.
     *
     * @param visitor the visitor
     */
    public static void visitAll(Visitor<?> visitor) {
        visitor.visit((FilterIsNull) null);
        visitor.visit((FilterComparison) null);
        visitor.visit((FilterIn) null);
        visitor.visit((FilterNot<?>) null);
        visitor.visit((FilterOr) null);
        visitor.visit((FilterAnd) null);
        visitor.visit((FilterPattern) null);
        visitor.visit((FilterQuick) null);
        visitor.visit((Function) null);
        visitor.visit((Method) null);
        visitor.visit(false);
        visitor.visit((RawString) null);
    }

    private enum FilterSpecificString implements Filter.Visitor<String> {
        INSTANCE;

        @Override
        public String visit(FilterIsNull isNull) {
            return of(isNull);
        }

        @Override
        public String visit(FilterComparison comparison) {
            return of(comparison);
        }

        @Override
        public String visit(FilterIn in) {
            return of(in);
        }


        @Override
        public String visit(FilterNot<?> not) {
            return of(not);
        }

        @Override
        public String visit(FilterOr ors) {
            return of(ors);
        }

        @Override
        public String visit(FilterAnd ands) {
            return of(ands);
        }

        @Override
        public String visit(FilterPattern pattern) {
            return of(pattern);
        }

        @Override
        public String visit(FilterQuick quick) {
            return of(quick);
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
        public String visit(boolean literal) {
            return of(literal);
        }

        @Override
        public String visit(RawString rawString) {
            return of(rawString);
        }
    }

    private static class CountingVisitor implements Visitor<CountingVisitor> {
        private int count = 0;

        @Override
        public CountingVisitor visit(FilterIsNull isNull) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(FilterComparison comparison) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(FilterIn in) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(FilterNot<?> not) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(FilterOr ors) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(FilterAnd ands) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(FilterPattern pattern) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(FilterQuick quick) {
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
        public CountingVisitor visit(boolean literal) {
            ++count;
            return this;
        }

        @Override
        public CountingVisitor visit(RawString rawString) {
            ++count;
            return this;
        }
    }
}
