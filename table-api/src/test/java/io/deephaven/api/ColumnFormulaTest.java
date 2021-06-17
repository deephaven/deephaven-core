package io.deephaven.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import org.junit.jupiter.api.Test;

public class ColumnFormulaTest {

    private static final ColumnFormula FOO_EQ_BAR =
        ColumnFormula.of(ColumnName.of("Foo"), ColumnName.of("Bar"));

    private static final ColumnFormula FOO_EQ_FOO_PLUS_1 =
        ColumnFormula.of(ColumnName.of("Foo"), RawString.of("Foo + 1"));

    private static final String FOO_EQ_BAR_STR = "Foo=Bar";

    private static final String FOO_EQ_FOO_PLUS_1_STR = "Foo=Foo + 1";

    @Test
    void strings() {
        assertThat(Strings.of(FOO_EQ_BAR)).isEqualTo(FOO_EQ_BAR_STR);
        assertThat(Strings.of(FOO_EQ_FOO_PLUS_1)).isEqualTo(FOO_EQ_FOO_PLUS_1_STR);
    }

    @Test
    void parsing() {
        assertThat(ColumnFormula.parse(FOO_EQ_BAR_STR)).isEqualTo(FOO_EQ_BAR);
        assertThat(ColumnFormula.parse(FOO_EQ_FOO_PLUS_1_STR)).isEqualTo(FOO_EQ_FOO_PLUS_1);
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
        // potential place for improvement
        assertThat(ColumnFormula.parse("Foo=Bar=Baz"))
            .isEqualTo(ColumnFormula.of(ColumnName.of("Foo"), RawString.of("Bar=Baz")));
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
    void rhsWhitespace() {
        // potential place for improvement
        assertThat(ColumnFormula.parse("Foo= Bar"))
            .isEqualTo(ColumnFormula.of(ColumnName.of("Foo"), RawString.of(" Bar")));
    }

    @Test
    void lhsWhitespace() {
        expectParseFailure("Foo =Bar");
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
            ColumnFormula.parse(x);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }
}
