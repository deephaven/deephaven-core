//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.api.filter.FilterComparison;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class SelectableTest {

    private static final ColumnName FOO = ColumnName.of("Foo");
    private static final ColumnName BAR = ColumnName.of("Bar");
    private static final ColumnName BAZ = ColumnName.of("Baz");
    private static final Selectable FOO_BAR = Selectable.of(FOO, BAR);
    private static final Selectable FOO_EXP = Selectable.of(FOO, RawString.of("foo(Bar) + 42"));

    @Test
    void newColumn() {
        assertThat(FOO.newColumn()).isEqualTo(FOO);
        assertThat(FOO_BAR.newColumn()).isEqualTo(FOO);
        assertThat(FOO_EXP.newColumn()).isEqualTo(FOO);
    }

    @Test
    void expressionColumn() {
        assertThat(FOO.expression()).isEqualTo(FOO);
        assertThat(FOO_BAR.expression()).isEqualTo(BAR);
        assertThat(FOO_EXP.expression()).isEqualTo(RawString.of("foo(Bar) + 42"));
    }

    @Test
    void fooFooIsFoo() {
        assertThat(Selectable.of(FOO, FOO)).isEqualTo(FOO);
    }

    @Test
    void selectColumnSameToString() {
        toString(FOO, "Foo");
    }

    @Test
    void selectColumnDifferentToString() {
        toString(FOO_BAR, "Foo=Bar");
    }

    @Test
    void selectExpressionToString() {
        toString(FOO_EXP, "Foo=foo(Bar) + 42");
    }

    @Test
    void fooEqBarGtBaz() {
        toString(Selectable.of(FOO, FilterComparison.gt(BAR, BAZ)), "Foo=Bar > Baz");
    }

    @Test
    void parseColumnSame() {
        parsable("Foo", FOO);
        parsable(" Foo", FOO);
        parsable("Foo ", FOO);
        parsable(" Foo ", FOO);
    }

    @Test
    void parseColumnDifferent() {
        parsable("Foo=Bar", Selectable.of(FOO, RawString.of("Bar")));
        parsable("Foo = Bar", Selectable.of(FOO, RawString.of(" Bar")));
    }

    @Test
    void parseExpression() {
        parsable("Foo=foo(Bar) + 42", FOO_EXP);
        parsable("Foo =foo(Bar) + 42", FOO_EXP);
        parsable(" Foo=foo(Bar) + 42", FOO_EXP);
        parsable(" Foo =foo(Bar) + 42", FOO_EXP);
    }

    @Test
    void invalid() {
        invalid("1BadColumn");
        invalid("Foo=");
        invalid("Foo==Bar");
        invalid("Foo==");
    }

    private static void parsable(String x, Selectable expected) {
        assertThat(Selectable.parse(x)).isEqualTo(expected);
    }

    private static void invalid(String x) {
        try {
            Selectable.parse(x);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static void toString(Selectable selectable, String expected) {
        assertThat(Strings.of(selectable)).isEqualTo(expected);
    }
}
