//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import org.junit.jupiter.api.Test;

import static io.deephaven.function.ToCharFunction.map;
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
