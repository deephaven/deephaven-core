package io.deephaven.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import org.junit.jupiter.api.Test;

public class ColumnNameTest {

    private static final ColumnName FOO = ColumnName.of("Foo");
    private static final ColumnName BAR = ColumnName.of("Bar");

    private static final String FOO_STR = "Foo";
    private static final String BAR_STR = "Bar";

    @Test
    void strings() {
        assertThat(Strings.of(FOO)).isEqualTo(FOO_STR);
        assertThat(Strings.of(BAR)).isEqualTo(BAR_STR);
    }

    @Test
    void parsing() {
        assertThat(ColumnName.of(FOO_STR)).isEqualTo(FOO);
        assertThat(ColumnName.of(BAR_STR)).isEqualTo(BAR);
    }

    @Test
    void name() {
        assertThat(FOO.name()).isEqualTo("Foo");
    }

    @Test
    void empty() {
        invalid("");
    }

    @Test
    void leadingWhitespace() {
        invalid(" Foo");
    }

    @Test
    void trailingWhitespace() {
        invalid("Foo ");
    }

    @Test
    void innerWhitespace() {
        invalid("Foo Bar");
    }

    @Test
    void withEquals() {
        invalid("Foo=Bar");
    }

    private void invalid(String x) {
        try {
            ColumnName.of(x);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }
}
