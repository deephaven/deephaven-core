package io.deephaven.functions;

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.deephaven.functions.ToBooleanFunction.and;
import static io.deephaven.functions.ToBooleanFunction.map;
import static io.deephaven.functions.ToBooleanFunction.not;
import static io.deephaven.functions.ToBooleanFunction.ofFalse;
import static io.deephaven.functions.ToBooleanFunction.ofTrue;
import static io.deephaven.functions.ToBooleanFunction.or;
import static org.assertj.core.api.Assertions.assertThat;

public class ToBooleanFunctionTest {

    @Test
    void ofTrue_() {
        assertThat(ofTrue().test(new Object())).isTrue();
    }

    @Test
    void ofFalse_() {
        assertThat(ofFalse().test(new Object())).isFalse();
    }

    @Test
    void or_() {
        assertThat(or(List.of()).test(new Object())).isFalse();
        assertThat(or(List.of(ofFalse())).test(new Object())).isFalse();
        assertThat(or(List.of(ofTrue())).test(new Object())).isTrue();
        assertThat(or(List.of(ofFalse(), ofFalse())).test(new Object())).isFalse();
        assertThat(or(List.of(ofFalse(), ofTrue())).test(new Object())).isTrue();
        assertThat(or(List.of(ofTrue(), ofFalse())).test(new Object())).isTrue();
        assertThat(or(List.of(ofTrue(), ofTrue())).test(new Object())).isTrue();
    }

    @Test
    void and_() {
        assertThat(and(List.of()).test(new Object())).isTrue();
        assertThat(and(List.of(ofFalse())).test(new Object())).isFalse();
        assertThat(and(List.of(ofTrue())).test(new Object())).isTrue();
        assertThat(and(List.of(ofFalse(), ofFalse())).test(new Object())).isFalse();
        assertThat(and(List.of(ofFalse(), ofTrue())).test(new Object())).isFalse();
        assertThat(and(List.of(ofTrue(), ofFalse())).test(new Object())).isFalse();
        assertThat(and(List.of(ofTrue(), ofTrue())).test(new Object())).isTrue();
    }

    @Test
    void not_() {
        assertThat(not(ofTrue()).test(new Object())).isFalse();
        assertThat(not(ofFalse()).test(new Object())).isTrue();
    }

    @Test
    void map_() {
        final ToBooleanFunction<String> trimIsFoo = map(String::trim, "foo"::equals);
        assertThat(trimIsFoo.test("")).isFalse();
        assertThat(trimIsFoo.test("  ")).isFalse();
        assertThat(trimIsFoo.test("foo")).isTrue();
        assertThat(trimIsFoo.test(" foo ")).isTrue();
        assertThat(trimIsFoo.test(" foo bar")).isFalse();
    }
}
