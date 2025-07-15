//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter.Visitor;
import io.deephaven.api.filter.FilterPattern.Mode;
import io.deephaven.api.literal.Literal;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

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
        stringsOf(isNull(FOO), "isNull(Foo)");
        stringsOf(not(isNull(FOO)), "!isNull(Foo)");
    }

    @Test
    void filterIsNotNull() {
        stringsOf(isNotNull(FOO), "!isNull(Foo)");
        stringsOf(not(isNotNull(FOO)), "isNull(Foo)");
    }

    @Test
    void filterNot() {
        stringsOf(not(isNull(FOO)), "!isNull(Foo)");
    }

    @Test
    void filterAnd() {
        stringsOf(and(isNotNull(FOO), isNotNull(BAR)), "!isNull(Foo) && !isNull(Bar)");
        stringsOf(not(and(isNotNull(FOO), isNotNull(BAR))), "isNull(Foo) || isNull(Bar)");
    }

    @Test
    void filterOr() {
        stringsOf(or(isNull(FOO), gt(FOO, BAR)), "isNull(Foo) || (Foo > Bar)");
        stringsOf(not(or(isNull(FOO), gt(FOO, BAR))), "!isNull(Foo) && (Foo <= Bar)");
    }

    @Test
    void filterOfTrue() {
        stringsOf(ofTrue(), "true");
        stringsOf(not(ofTrue()), "false");
    }

    @Test
    void filterOfFalse() {
        stringsOf(ofFalse(), "false");
        stringsOf(not(ofFalse()), "true");
    }

    @Test
    void filterIsTrue() {
        stringsOf(isTrue(FOO), "Foo == true");
        stringsOf(not(isTrue(FOO)), "Foo != true");
    }

    @Test
    void filterIsFalse() {
        stringsOf(isFalse(FOO), "Foo == false");
        stringsOf(not(isFalse(FOO)), "Foo != false");
    }

    @Test
    void filterEqPrecedence() {
        stringsOf(eq(or(isTrue(FOO), eq(BAR, BAZ)), and(isTrue(FOO), neq(BAR, BAZ))),
                "((Foo == true) || (Bar == Baz)) == ((Foo == true) && (Bar != Baz))");
        stringsOf(not(eq(or(isTrue(FOO), eq(BAR, BAZ)), and(isTrue(FOO), neq(BAR, BAZ)))),
                "((Foo == true) || (Bar == Baz)) != ((Foo == true) && (Bar != Baz))");
    }

    @Test
    void filterFunction() {
        stringsOf(Function.of("MyFunction1"), "MyFunction1()");
        stringsOf(Function.of("MyFunction2", FOO), "MyFunction2(Foo)");
        stringsOf(Function.of("MyFunction3", FOO, BAR), "MyFunction3(Foo, Bar)");

        stringsOf(not(Function.of("MyFunction1")), "!MyFunction1()");
        stringsOf(not(Function.of("MyFunction2", FOO)), "!MyFunction2(Foo)");
        stringsOf(not(Function.of("MyFunction3", FOO, BAR)), "!MyFunction3(Foo, Bar)");
    }

    @Test
    void filterMethod() {
        stringsOf(Method.of(FOO, "MyFunction1"), "Foo.MyFunction1()");
        stringsOf(Method.of(FOO, "MyFunction2", BAR), "Foo.MyFunction2(Bar)");
        stringsOf(Method.of(FOO, "MyFunction3", BAR, BAZ), "Foo.MyFunction3(Bar, Baz)");

        stringsOf(not(Method.of(FOO, "MyFunction1")), "!Foo.MyFunction1()");
        stringsOf(not(Method.of(FOO, "MyFunction2", BAR)), "!Foo.MyFunction2(Bar)");
        stringsOf(not(Method.of(FOO, "MyFunction3", BAR, BAZ)), "!Foo.MyFunction3(Bar, Baz)");
    }

    @Test
    void filterPattern() {
        stringsOf(FilterPattern.of(FOO, Pattern.compile("myregex"), Mode.FIND, false),
                "FilterPattern(ColumnName(Foo), myregex, 0, FIND, false)");
        stringsOf(FilterPattern.of(FOO, Pattern.compile("myregex"), Mode.FIND, true),
                "FilterPattern(ColumnName(Foo), myregex, 0, FIND, true)");

        stringsOf(not(FilterPattern.of(FOO, Pattern.compile("myregex"), Mode.FIND, false)),
                "!FilterPattern(ColumnName(Foo), myregex, 0, FIND, false)");
        stringsOf(not(FilterPattern.of(FOO, Pattern.compile("myregex"), Mode.FIND, true)),
                "!FilterPattern(ColumnName(Foo), myregex, 0, FIND, true)");
    }

    @Test
    void filterSerial() {
        stringsOf(Function.of("MySerialFunction").withSerial(), "invokeSerially(MySerialFunction())");
    }

    @Test
    void filterIn() {
        stringsOf(FilterIn.of(FOO, Literal.of(40), Literal.of(42)),
                "FilterIn{expression=ColumnName(Foo), values=[LiteralInt{value=40}, LiteralInt{value=42}]}");
        stringsOf(not(FilterIn.of(FOO, Literal.of(40), Literal.of(42))),
                "!FilterIn{expression=ColumnName(Foo), values=[LiteralInt{value=40}, LiteralInt{value=42}]}");
    }

    @Test
    void filterRawString() {
        stringsOf(RawString.of("this is a raw string"), "this is a raw string");
        stringsOf(Filter.not(RawString.of("this is a raw string")), "!(this is a raw string)");
    }

    @Test
    void examplesStringsOf() {
        for (Filter filter : Examples.of()) {
            of(filter);
        }
    }

    @Test
    void extractAnds() {
        for (Filter filter : Examples.of()) {
            final Collection<Filter> results = ExtractAnds.of(filter);
            if (filter instanceof FilterAnd) {
                assertThat(results).isEqualTo(((FilterAnd) filter).filters());
            } else if (Filter.ofTrue().equals(filter)) {
                assertThat(results).isEmpty();
            } else if (filter instanceof FilterBarrier) {
                assertThat(results).containsExactly(((FilterBarrier) filter).filter());
            } else if (filter instanceof FilterRespectsBarrier) {
                assertThat(results).containsExactly(((FilterRespectsBarrier) filter).filter());
            } else {
                assertThat(results).containsExactly(filter);
            }
        }
    }

    private static void stringsOf(Filter filter, String expected) {
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
        visitor.visit((FilterSerial) null);
        visitor.visit((FilterBarrier) null);
        visitor.visit((FilterRespectsBarrier) null);
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
        public String visit(FilterSerial serial) {
            return of(serial);
        }

        @Override
        public String visit(FilterBarrier barrier) {
            return of(barrier);
        }

        @Override
        public String visit(FilterRespectsBarrier respectsBarrier) {
            return of(respectsBarrier);
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
        public CountingVisitor visit(FilterSerial serial) {
            ++count;
            return null;
        }

        @Override
        public CountingVisitor visit(FilterBarrier barrier) {
            ++count;
            return null;
        }

        @Override
        public CountingVisitor visit(FilterRespectsBarrier respectsBarrier) {
            ++count;
            return null;
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

    public static class Examples implements Filter.Visitor<Void> {

        public static List<Filter> of() {
            Examples visitor = new Examples();
            visitAll(visitor);
            final List<Filter> out = new ArrayList<>(visitor.out);
            for (Filter filter : visitor.out) {
                out.add(Filter.not(filter));
                out.add(Filter.not(Filter.not(filter)));
            }
            return visitor.out;
        }

        private final List<Filter> out = new ArrayList<>();

        @Override
        public Void visit(FilterIsNull isNull) {
            out.add(FilterIsNull.of(FOO));
            return null;
        }

        @Override
        public Void visit(FilterComparison comparison) {
            out.add(FilterComparison.eq(FOO, BAR));
            out.add(FilterComparison.gt(FOO, BAR));
            out.add(FilterComparison.geq(FOO, BAR));
            out.add(FilterComparison.lt(FOO, BAR));
            out.add(FilterComparison.leq(FOO, BAR));
            out.add(FilterComparison.neq(FOO, BAR));
            return null;
        }

        @Override
        public Void visit(FilterIn in) {
            out.add(FilterIn.of(FOO, Literal.of(40), Literal.of(42)));
            return null;
        }

        @Override
        public Void visit(FilterNot<?> not) {
            // all filters not will be handled in of()
            return null;
        }

        @Override
        public Void visit(FilterOr ors) {
            out.add(FilterOr.of(Filter.isTrue(FOO), Filter.isTrue(BAR)));
            return null;
        }

        @Override
        public Void visit(FilterAnd ands) {
            out.add(FilterAnd.of(Filter.isTrue(FOO), Filter.isTrue(BAR)));
            return null;
        }

        @Override
        public Void visit(FilterPattern pattern) {
            out.add(FilterPattern.of(FOO, Pattern.compile("somepattern"), Mode.FIND, false));
            out.add(FilterPattern.of(FOO, Pattern.compile("somepattern"), Mode.FIND, true));
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
        public Void visit(boolean literal) {
            out.add(Filter.ofFalse());
            out.add(Filter.ofTrue());
            return null;
        }

        @Override
        public Void visit(FilterSerial serial) {
            out.add(Function.of("my_serial_function", FOO).withSerial());
            return null;
        }

        @Override
        public Void visit(FilterBarrier barrier) {
            out.add(Function.of("my_serial_function", FOO).withBarriers("TEST_BARRIER"));
            return null;
        }

        @Override
        public Void visit(FilterRespectsBarrier respectsBarrier) {
            out.add(Function.of("my_serial_function", FOO).respectsBarriers("TEST_BARRIER"));
            return null;
        }

        @Override
        public Void visit(RawString rawString) {
            out.add(RawString.of("Foo > Bar"));
            out.add(RawString.of("Foo >= Bar + 42"));
            out.add(RawString.of("aoeuaoeu"));
            return null;
        }
    }
}
