package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ByteArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(ByteArray.of((byte) 1, null, (byte) 3).values()).containsExactly((byte) 1,
            Util.NULL_BYTE, (byte) 3);
    }

    @Test
    void rawInRawOut() {
        assertThat(ByteArray.ofUnsafe((byte) 1, Util.NULL_BYTE, (byte) 3).values())
            .containsExactly((byte) 1, Util.NULL_BYTE, (byte) 3);
    }
}
