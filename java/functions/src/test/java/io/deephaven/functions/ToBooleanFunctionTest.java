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

    private static final Object OBJ = new Object();

    // Specifically create versions that aren't the library's internal objects

    private static <T> ToBooleanFunction<T> myTrue() {
        return x -> true;
    }

    private static <T> ToBooleanFunction<T> myFalse() {
        return x -> false;
    }

    @Test
    void ofTrue_() {
        isTrue(ofTrue(), OBJ);
        isTrue(myTrue(), OBJ);
    }

    @Test
    void ofFalse_() {
        isFalse(ofFalse(), OBJ);
        isFalse(myFalse(), OBJ);
    }

    @Test
    void or_() {
        isFalse(or(List.of()), OBJ);

        isFalse(or(List.of(ofFalse())), OBJ);
        isTrue(or(List.of(ofTrue())), OBJ);
        isFalse(or(List.of(ofFalse(), ofFalse())), OBJ);
        isTrue(or(List.of(ofFalse(), ofTrue())), OBJ);
        isTrue(or(List.of(ofTrue(), ofFalse())), OBJ);
        isTrue(or(List.of(ofTrue(), ofTrue())), OBJ);

        isFalse(or(List.of(myFalse())), OBJ);
        isTrue(or(List.of(myTrue())), OBJ);
        isFalse(or(List.of(myFalse(), myFalse())), OBJ);
        isTrue(or(List.of(myFalse(), myTrue())), OBJ);
        isTrue(or(List.of(myTrue(), myFalse())), OBJ);
        isTrue(or(List.of(myTrue(), myTrue())), OBJ);
    }

    @Test
    void and_() {
        isTrue(and(List.of()), OBJ);

        isFalse(and(List.of(ofFalse())), OBJ);
        isTrue(and(List.of(ofTrue())), OBJ);
        isFalse(and(List.of(ofFalse(), ofFalse())), OBJ);
        isFalse(and(List.of(ofFalse(), ofTrue())), OBJ);
        isFalse(and(List.of(ofTrue(), ofFalse())), OBJ);
        isTrue(and(List.of(ofTrue(), ofTrue())), OBJ);

        isFalse(and(List.of(myFalse())), OBJ);
        isTrue(and(List.of(myTrue())), OBJ);
        isFalse(and(List.of(myFalse(), myFalse())), OBJ);
        isFalse(and(List.of(myFalse(), myTrue())), OBJ);
        isFalse(and(List.of(myTrue(), myFalse())), OBJ);
        isTrue(and(List.of(myTrue(), myTrue())), OBJ);
    }

    @Test
    void not_() {
        isFalse(not(ofTrue()), OBJ);
        isTrue(not(ofFalse()), OBJ);

        isFalse(not(myTrue()), OBJ);
        isTrue(not(myFalse()), OBJ);
    }

    @Test
    void map_() {
        final ToBooleanFunction<String> trimIsFoo = map(String::trim, "foo"::equals);
        isFalse(trimIsFoo, "");
        isFalse(trimIsFoo, "  ");
        isTrue(trimIsFoo, "foo");
        isTrue(trimIsFoo, " foo ");
        isFalse(trimIsFoo, " foo bar");
    }

    private static <X> void isTrue(ToBooleanFunction<X> f, X x) {
        assertThat(f.test(x)).isTrue();
        assertThat(f.negate().test(x)).isFalse();

        assertThat(f.or(ofTrue()).test(x)).isTrue();
        assertThat(f.and(ofTrue()).test(x)).isTrue();
        assertThat(f.or(ofFalse()).test(x)).isTrue();
        assertThat(f.and(ofFalse()).test(x)).isFalse();

        assertThat(ToBooleanFunction.<X>ofTrue().or(f).test(x)).isTrue();
        assertThat(ToBooleanFunction.<X>ofTrue().and(f).test(x)).isTrue();
        assertThat(ToBooleanFunction.<X>ofFalse().or(f).test(x)).isTrue();
        assertThat(ToBooleanFunction.<X>ofFalse().and(f).test(x)).isFalse();

        assertThat(f.or(myTrue()).test(x)).isTrue();
        assertThat(f.and(myTrue()).test(x)).isTrue();
        assertThat(f.or(myFalse()).test(x)).isTrue();
        assertThat(f.and(myFalse()).test(x)).isFalse();

        assertThat(ToBooleanFunctionTest.<X>myTrue().or(f).test(x)).isTrue();
        assertThat(ToBooleanFunctionTest.<X>myTrue().and(f).test(x)).isTrue();
        assertThat(ToBooleanFunctionTest.<X>myFalse().or(f).test(x)).isTrue();
        assertThat(ToBooleanFunctionTest.<X>myFalse().and(f).test(x)).isFalse();
    }

    private static <X> void isFalse(ToBooleanFunction<X> f, X x) {
        assertThat(f.test(x)).isFalse();
        assertThat(f.negate().test(x)).isTrue();

        assertThat(f.or(ofTrue()).test(x)).isTrue();
        assertThat(f.and(ofTrue()).test(x)).isFalse();
        assertThat(f.or(ofFalse()).test(x)).isFalse();
        assertThat(f.and(ofFalse()).test(x)).isFalse();

        assertThat(ToBooleanFunction.<X>ofTrue().or(f).test(x)).isTrue();
        assertThat(ToBooleanFunction.<X>ofTrue().and(f).test(x)).isFalse();
        assertThat(ToBooleanFunction.<X>ofFalse().or(f).test(x)).isFalse();
        assertThat(ToBooleanFunction.<X>ofFalse().and(f).test(x)).isFalse();

        assertThat(f.or(myTrue()).test(x)).isTrue();
        assertThat(f.and(myTrue()).test(x)).isFalse();
        assertThat(f.or(myFalse()).test(x)).isFalse();
        assertThat(f.and(myFalse()).test(x)).isFalse();

        assertThat(ToBooleanFunctionTest.<X>myTrue().or(f).test(x)).isTrue();
        assertThat(ToBooleanFunctionTest.<X>myTrue().and(f).test(x)).isFalse();
        assertThat(ToBooleanFunctionTest.<X>myFalse().or(f).test(x)).isFalse();
        assertThat(ToBooleanFunctionTest.<X>myFalse().and(f).test(x)).isFalse();
    }
}
