package io.deephaven.functions;

import org.junit.jupiter.api.Test;

import static io.deephaven.functions.ToShortFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToShortFunctionTest {

    @Test
    void map_() {
        final ToShortFunction<String> bytesLength = map(String::getBytes, x -> (short) x.length);
        assertThat(bytesLength.applyAsShort("foo")).isEqualTo((short) 3);
        assertThat(bytesLength.applyAsShort("food")).isEqualTo((short) 4);
    }
}
