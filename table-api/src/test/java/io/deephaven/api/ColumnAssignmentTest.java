package io.deephaven.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import org.junit.jupiter.api.Test;

public class ColumnAssignmentTest {

    private static final ColumnAssignment FOO_EQ_BAR =
        ColumnAssignment.of(ColumnName.of("Foo"), ColumnName.of("Bar"));

    private static final ColumnAssignment FOO_EQ_FOO =
        ColumnAssignment.of(ColumnName.of("Foo"), ColumnName.of("Foo"));

    private static final String FOO_EQ_BAR_STR = "Foo=Bar";

    private static final String FOO_EQ_FOO_STR = "Foo=Foo";

    @Test
    void strings() {
        assertThat(Strings.of(FOO_EQ_BAR)).isEqualTo(FOO_EQ_BAR_STR);
        assertThat(Strings.of(FOO_EQ_FOO)).isEqualTo(FOO_EQ_FOO_STR);
    }

    @Test
    void parsing() {
        parse(FOO_EQ_BAR_STR, FOO_EQ_BAR);
        parse(FOO_EQ_FOO_STR, FOO_EQ_FOO);
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
    void doubleEq() {
        expectParseFailure("Foo==Bar");
    }

    @Test
    void multiAssign() {
        expectParseFailure("Foo=Bar=Baz");
    }

    @Test
    void noLHS() {
        expectParseFailure("=Bar");
    }

    @Test
    void noRHS() {
        expectParseFailure("Foo=");
    }

    @Test
    void rhsLeadingWhitespace() {
        parse("Foo= Bar", FOO_EQ_BAR);
    }

    @Test
    void rhsTrailingWhitespace() {
        parse("Foo=Bar ", FOO_EQ_BAR);
    }

    @Test
    void lhsLeadingWhitespace() {
        parse(" Foo=Bar", FOO_EQ_BAR);
    }

    @Test
    void lhsTrailingWhitespace() {
        parse("Foo =Bar", FOO_EQ_BAR);
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

    private void parse(String x, ColumnAssignment expected) {
        assertThat(ColumnAssignment.parse(x)).isEqualTo(expected);
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
