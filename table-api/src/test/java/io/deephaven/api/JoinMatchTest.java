package io.deephaven.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import org.junit.jupiter.api.Test;

public class JoinMatchTest {

    public static final JoinMatch FOO = ColumnName.of("Foo");
    public static final JoinMatch FOO_BAR =
        JoinMatch.of(ColumnName.of("Foo"), ColumnName.of("Bar"));

    @Test
    void left() {
        assertThat(FOO.left()).isEqualTo(ColumnName.of("Foo"));
        assertThat(FOO_BAR.left()).isEqualTo(ColumnName.of("Foo"));
    }

    @Test
    void right() {
        assertThat(FOO.right()).isEqualTo(ColumnName.of("Foo"));
        assertThat(FOO_BAR.right()).isEqualTo(ColumnName.of("Bar"));
    }

    @Test
    void columnSame() {
        assertThat(toString(FOO)).isEqualTo("Foo");
    }

    @Test
    void columnDifferent() {
        assertThat(toString(FOO_BAR)).isEqualTo("Foo==Bar");
    }

    @Test
    void parseColumnSame() {
        parsable("Foo", FOO);
        parsable("Foo ", FOO);
        parsable(" Foo", FOO);
        parsable(" Foo ", FOO);
    }

    @Test
    void parseColumnDifferent() {
        parsable("Foo=Bar", FOO_BAR);
        parsable("Foo==Bar", FOO_BAR);
        parsable(" Foo = Bar ", FOO_BAR);
        parsable(" Foo == Bar ", FOO_BAR);
    }

    @Test
    void invalid() {
        invalid("Foo=");
        invalid("Foo==");
        invalid("=Bar");
        invalid("==Bar");
    }

    private static void parsable(String x, JoinMatch expected) {
        assertThat(JoinMatch.parse(x)).isEqualTo(expected);
    }

    private static void invalid(String x) {
        try {
            JoinMatch.parse(x);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static String toString(JoinMatch match) {
        return Strings.of(match);
    }
}
