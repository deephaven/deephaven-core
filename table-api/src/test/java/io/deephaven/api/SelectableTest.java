package io.deephaven.api;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class SelectableTest {

    private static final Selectable FOO = ColumnName.of("Foo");
    private static final Selectable FOO_BAR =
        Selectable.of(ColumnName.of("Foo"), ColumnName.of("Bar"));
    private static final Selectable FOO_EXP =
        Selectable.of(ColumnName.of("Foo"), RawString.of("foo(Bar) + 42"));

    @Test
    void newColumn() {
        assertThat(FOO.newColumn()).isEqualTo(ColumnName.of("Foo"));
        assertThat(FOO_BAR.newColumn()).isEqualTo(ColumnName.of("Foo"));
        assertThat(FOO_EXP.newColumn()).isEqualTo(ColumnName.of("Foo"));
    }

    @Test
    void expressionColumn() {
        assertThat(FOO.expression()).isEqualTo(ColumnName.of("Foo"));
        assertThat(FOO_BAR.expression()).isEqualTo(ColumnName.of("Bar"));
        assertThat(FOO_EXP.expression()).isEqualTo(RawString.of("foo(Bar) + 42"));
    }

    @Test
    void fooFooIsFoo() {
        assertThat(Selectable.of(ColumnName.of("Foo"), ColumnName.of("Foo"))).isEqualTo(FOO);
    }

    @Test
    void selectColumnSameToString() {
        assertThat(toString(FOO)).isEqualTo("Foo");
    }

    @Test
    void selectColumnDifferentToString() {
        assertThat(toString(FOO_BAR)).isEqualTo("Foo=Bar");
    }

    @Test
    void selectExpressionToString() {
        assertThat(toString(FOO_EXP)).isEqualTo("Foo=foo(Bar) + 42");
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
        parsable("Foo=Bar", Selectable.of(ColumnName.of("Foo"), RawString.of("Bar")));
        parsable("Foo = Bar", Selectable.of(ColumnName.of("Foo"), RawString.of(" Bar")));
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

    private static String toString(Selectable selectable) {
        return Strings.of(selectable);
    }
}
