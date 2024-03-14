//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import org.junit.jupiter.api.Test;

import static io.deephaven.function.ToShortFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToShortFunctionTest {

    @Test
    void map_() {
        final ToShortFunction<String> bytesLength = map(String::getBytes, x -> (short) x.length);
        assertThat(bytesLength.applyAsShort("foo")).isEqualTo((short) 3);
        assertThat(bytesLength.applyAsShort("food")).isEqualTo((short) 4);
    }
}
