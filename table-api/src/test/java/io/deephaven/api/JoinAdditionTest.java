package io.deephaven.api;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class JoinAdditionTest {

    public static final JoinAddition FOO = ColumnName.of("Foo");
    public static final JoinAddition FOO_BAR =
            JoinAddition.of(ColumnName.of("Foo"), ColumnName.of("Bar"));

    @Test
    void newColumn() {
        assertThat(FOO.newColumn()).isEqualTo(ColumnName.of("Foo"));
        assertThat(FOO_BAR.newColumn()).isEqualTo(ColumnName.of("Foo"));
    }

    @Test
    void existingColumn() {
        assertThat(FOO.existingColumn()).isEqualTo(ColumnName.of("Foo"));
        assertThat(FOO_BAR.existingColumn()).isEqualTo(ColumnName.of("Bar"));
    }

    @Test
    void columnSame() {
        assertThat(toString(FOO)).isEqualTo("Foo");
    }

    @Test
    void columnDifferent() {
        assertThat(toString(FOO_BAR)).isEqualTo("Foo=Bar");
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
        parsable(" Foo = Bar ", FOO_BAR);
    }

    @Test
    void invalid() {
        invalid("Foo==Bar");
        invalid("Foo=");
        invalid("Foo==");
        invalid("=Bar");
        invalid("==Bar");
    }

    private static void parsable(String x, JoinAddition expected) {
        assertThat(JoinAddition.parse(x)).isEqualTo(expected);
    }

    private static void invalid(String x) {
        try {
            JoinAddition.parse(x);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static String toString(JoinAddition match) {
        return Strings.of(match);
    }
}
