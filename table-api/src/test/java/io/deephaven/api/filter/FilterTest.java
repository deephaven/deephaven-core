/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.IfThenElse;
import io.deephaven.api.expression.Method;
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


    @Test
    void filterIsNull() {
        toString(isNull(FOO), "isNull(Foo)");
    }

    @Test
    void filterIsNotNull() {
        toString(isNotNull(FOO), "!isNull(Foo)");
    }

    @Test
    void filterNot() {
        toString(not(isNull(FOO)), "!isNull(Foo)");
    }

    @Test
    void filterAnd() {
        toString(and(isNotNull(FOO), isNotNull(BAR)), "!isNull(Foo) && !isNull(Bar)");
    }

    @Test
    void filterOr() {
        toString(or(isNull(FOO), gt(FOO, BAR)), "isNull(Foo) || (Foo > Bar)");
    }

    @Test
    void filterOfTrue() {
        toString(ofTrue(), "true");
    }

    @Test
    void filterOfFalse() {
        toString(ofFalse(), "false");
    }

    @Test
    void filterIsTrue() {
        toString(isTrue(FOO), "Foo == true");
    }

    @Test
    void filterIsFalse() {
        toString(isFalse(FOO), "Foo == false");
    }

    @Test
    void filterColumnName() {
        toString(FOO, "Foo");
    }

    @Test
    void filterNotColumnName() {
        toString(not(FOO), "!Foo");
    }

    @Test
    void filterNotNotColumnName() {
        toString(not(not(FOO)), "!!Foo");
    }

    @Test
    void filterEqPrecedence() {
        toString(eq(or(FOO, eq(BAR, BAZ)), and(FOO, neq(BAR, BAZ))), "(Foo || (Bar == Baz)) == (Foo && (Bar != Baz))");
    }

    @Test
    void filterFunction() {
        toString(Function.of("MyFunction1"), "MyFunction1()");
        toString(Function.of("MyFunction2", FOO), "MyFunction2(Foo)");
        toString(Function.of("MyFunction3", FOO, BAR), "MyFunction3(Foo, Bar)");
    }

    @Test
    void filterMethod() {
        toString(Method.of(FOO, "MyFunction1"), "Foo.MyFunction1()");
        toString(Method.of(FOO, "MyFunction2", BAR), "Foo.MyFunction2(Bar)");
        toString(Method.of(FOO, "MyFunction3", BAR, BAZ), "Foo.MyFunction3(Bar, Baz)");
    }

    @Test
    void filterIfThenElse() {
        toString(IfThenElse.of(FOO, BAR, BAZ), "Foo ? Bar : Baz");
    }

    private static void toString(Filter filter, String expected) {
        assertThat(of(filter)).isEqualTo(expected);
        assertThat(filter.walk(FilterSpecificString.INSTANCE)).isEqualTo(expected);
    }

    private enum FilterSpecificString implements Filter.Visitor<String> {
        INSTANCE;

        @Override
        public String visit(FilterIsNull isNull) {
            return of(isNull);
        }

        @Override
        public String visit(FilterIsNotNull isNotNull) {
            return of(isNotNull);
        }

        @Override
        public String visit(FilterComparison comparison) {
            return of(comparison);
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
        public String visit(ColumnName columnName) {
            return of(columnName);
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
        public String visit(IfThenElse ifThenElse) {
            return of(ifThenElse);
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
}
