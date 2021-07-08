package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class PairTest {

    private static final Pair FOO_BAR = Pair.of(ColumnName.of("Bar"), ColumnName.of("Foo"));

    private static final Pair FOO = ColumnName.of("Foo");

    private static final String FOO_BAR_STR = "Foo=Bar";

    private static final String FOO_STR = "Foo";

    @Test
    void strings() {
        assertThat(Strings.of(FOO_BAR)).isEqualTo(FOO_BAR_STR);
        assertThat(Strings.of(FOO)).isEqualTo(FOO_STR);
    }

    @Test
    void parsing() {
        parse(FOO_BAR_STR, FOO_BAR);
        parse(FOO_STR, FOO);
    }

    @Test
    void input() {
        assertThat(FOO.input()).isEqualTo(ColumnName.of("Foo"));
        assertThat(FOO_BAR.input()).isEqualTo(ColumnName.of("Bar"));
    }

    @Test
    void output() {
        assertThat(FOO.output()).isEqualTo(ColumnName.of("Foo"));
        assertThat(FOO_BAR.output()).isEqualTo(ColumnName.of("Foo"));
    }

    @Test
    void empty() {
        invalid("");
    }

    @Test
    void noEq() {
        parse("FooBar", ColumnName.of("FooBar"));
    }

    @Test
    void doubleEq() {
        invalid("Foo==Bar");
    }

    @Test
    void multiAssign() {
        invalid("Foo=Bar=Baz");
    }

    @Test
    void noLHS() {
        invalid("=Bar");
    }

    @Test
    void noRHS() {
        invalid("Foo=");
    }

    @Test
    void rhsLeadingWhitespace() {
        parse("Foo= Bar", FOO_BAR);
    }

    @Test
    void rhsTrailingWhitespace() {
        parse("Foo=Bar ", FOO_BAR);
    }

    @Test
    void lhsLeadingWhitespace() {
        parse(" Foo=Bar", FOO_BAR);
    }

    @Test
    void lhsTrailingWhitespace() {
        parse("Foo =Bar", FOO_BAR);
    }

    @Test
    void notEq() {
        invalid("Foo!=Bar");
    }

    @Test
    void unicodeNotEq() {
        invalid("Foo≠Bar");
    }

    @Test
    void unicodeEquivalent() {
        invalid("Foo≡Bar");
    }

    private void parse(String x, Pair expected) {
        assertThat(Pair.parse(x)).isEqualTo(expected);
    }

    private void invalid(String x) {
        try {
            Pair.parse(x);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }
}
