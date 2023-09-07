package io.deephaven.functions;

import io.deephaven.qst.type.Type;
import org.junit.jupiter.api.Test;

import static io.deephaven.functions.ToObjectFunction.map;
import static org.assertj.core.api.Assertions.assertThat;

public class ToObjectFunctionTest {

    @Test
    void map_() {
        final ToObjectFunction<String, byte[]> bytesLength =
                map(String::getBytes, ToObjectFunctionTest::firstAndLast, Type.byteType().arrayType());
        assertThat(bytesLength.apply("f")).containsExactly((byte) 'f', (byte) 'f');
        assertThat(bytesLength.apply("foo")).containsExactly((byte) 'f', (byte) 'o');
        assertThat(bytesLength.apply("food")).containsExactly((byte) 'f', (byte) 'd');
    }

    private static byte[] firstAndLast(byte[] x) {
        return new byte[] {x[0], x[x.length - 1]};
    }
}
