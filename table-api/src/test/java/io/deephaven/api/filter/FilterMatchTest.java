package io.deephaven.api.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Strings;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class FilterMatchTest {

    private static final FilterMatch FOO_MATCH_BAR =
        FilterMatch.of(ColumnName.of("Foo"), ColumnName.of("Bar"));

    private static final FilterMatch FOO_MATCH_FOO =
        FilterMatch.of(ColumnName.of("Foo"), ColumnName.of("Foo"));

    private static final String FOO_MATCH_BAR_STR = "Foo==Bar";

    private static final String FOO_MATCH_FOO_STR = "Foo==Foo";

    @Test
    void strings() {
        Assertions.assertThat(Strings.of(FOO_MATCH_BAR)).isEqualTo(FOO_MATCH_BAR_STR);
        assertThat(Strings.of(FOO_MATCH_FOO)).isEqualTo(FOO_MATCH_FOO_STR);
    }

    @Test
    void parsing() {
        assertThat(FilterMatch.parse(FOO_MATCH_BAR_STR)).isEqualTo(FOO_MATCH_BAR);
        assertThat(FilterMatch.parse(FOO_MATCH_FOO_STR)).isEqualTo(FOO_MATCH_FOO);
    }

    @Test
    void singleEq() {
        assertThat(FilterMatch.parse("Foo=Bar")).isEqualTo(FOO_MATCH_BAR);
    }

    @Test
    void empty() {
        invalid("");
    }

    @Test
    void noEq() {
        invalid("FooBar");
    }

    @Test
    void multiMatch1() {
        invalid("Foo=Bar=Baz");
    }

    @Test
    void multiMatch2() {
        invalid("Foo==Bar==Baz");
    }

    @Test
    void tripleEq() {
        invalid("Foo===Bar");
    }

    @Test
    void noLHS1() {
        invalid("=Bar");
    }

    @Test
    void noLHS2() {
        invalid("==Bar");
    }

    @Test
    void noRHS1() {
        invalid("Foo=");
    }

    @Test
    void noRHS2() {
        invalid("Foo==");
    }

    @Test
    void rhsWhitespace() {
        assertThat(FilterMatch.parse("Foo== Bar")).isEqualTo(FOO_MATCH_BAR);
    }

    @Test
    void lhsWhitespace() {
        assertThat(FilterMatch.parse("Foo ==Bar")).isEqualTo(FOO_MATCH_BAR);
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

    private void invalid(String x) {
        try {
            FilterMatch.parse(x);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }
}
