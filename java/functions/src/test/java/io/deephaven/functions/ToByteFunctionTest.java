package io.deephaven.functions;

import org.junit.jupiter.api.Test;

import static io.deephaven.functions.ToByteFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToByteFunctionTest {

    @Test
    void map_() {
        final ToByteFunction<String> firstByte = map(String::getBytes, ToByteFunctionTest::firstByte);
        assertThat(firstByte.applyAsByte("foo")).isEqualTo((byte) 'f');
        assertThat(firstByte.applyAsByte("oof")).isEqualTo((byte) 'o');
    }

    private static byte firstByte(byte[] x) {
        return x[0];
    }
}
