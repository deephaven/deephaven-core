package io.deephaven.functions;

import org.junit.jupiter.api.Test;

import static io.deephaven.functions.ToCharFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToCharFunctionTest {

    @Test
    void map_() {
        final ToCharFunction<String> firstByte = map(String::toCharArray, ToCharFunctionTest::firstChar);
        assertThat(firstByte.applyAsChar("foo")).isEqualTo('f');
        assertThat(firstByte.applyAsChar("oof")).isEqualTo('o');
    }

    private static char firstChar(char[] x) {
        return x[0];
    }
}
