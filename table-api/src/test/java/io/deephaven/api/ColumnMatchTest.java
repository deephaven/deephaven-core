package io.deephaven.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import org.junit.jupiter.api.Test;

public class ColumnMatchTest {

    private static final ColumnMatch FOO_MATCH_BAR =
        ColumnMatch.of(ColumnName.of("Foo"), ColumnName.of("Bar"));

    private static final ColumnMatch FOO_MATCH_FOO =
        ColumnMatch.of(ColumnName.of("Foo"), ColumnName.of("Foo"));

    private static final String FOO_MATCH_BAR_STR = "Foo==Bar";

    private static final String FOO_MATCH_FOO_STR = "Foo==Foo";

    @Test
    void strings() {
        assertThat(Strings.of(FOO_MATCH_BAR)).isEqualTo(FOO_MATCH_BAR_STR);
        assertThat(Strings.of(FOO_MATCH_FOO)).isEqualTo(FOO_MATCH_FOO_STR);
    }

    @Test
    void parsing() {
        assertThat(ColumnMatch.parse(FOO_MATCH_BAR_STR)).isEqualTo(FOO_MATCH_BAR);
        assertThat(ColumnMatch.parse(FOO_MATCH_FOO_STR)).isEqualTo(FOO_MATCH_FOO);
    }

    @Test
    void singleEq() {
        assertThat(ColumnMatch.parse("Foo=Bar")).isEqualTo(FOO_MATCH_BAR);
    }

    @Test
    void empty() {
        expectParseFailure("");
    }

    @Test
    void noEq() {
        expectParseFailure("FooBar");
    }

    @Test
    void multiMatch1() {
        expectParseFailure("Foo=Bar=Baz");
    }

    @Test
    void multiMatch2() {
        expectParseFailure("Foo==Bar==Baz");
    }

    @Test
    void tripleEq() {
        expectParseFailure("Foo===Bar");
    }

    @Test
    void noLHS1() {
        expectParseFailure("=Bar");
    }

    @Test
    void noLHS2() {
        expectParseFailure("==Bar");
    }

    @Test
    void noRHS1() {
        expectParseFailure("Foo=");
    }

    @Test
    void noRHS2() {
        expectParseFailure("Foo==");
    }

    @Test
    void rhsWhitespace() {
        expectParseFailure("Foo== Bar");
    }

    @Test
    void lhsWhitespace() {
        expectParseFailure("Foo ==Bar");
    }

    @Test
    void notEq() {
        expectParseFailure("Foo!=Bar");
    }

    @Test
    void unicodeNotEq() {
        expectParseFailure("Foo≠Bar");
    }

    @Test
    void unicodeEquivalent() {
        expectParseFailure("Foo≡Bar");
    }

    private void expectParseFailure(String x) {
        try {
            ColumnAssignment.parse(x);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }
}
