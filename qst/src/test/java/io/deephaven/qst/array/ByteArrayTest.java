package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ByteArrayTest {
    @Test
    void boxed() {
        assertThat(ByteArray.of((byte) 1, null, (byte) 3)).containsExactly((byte) 1, null,
            (byte) 3);
    }

    @Test
    void raw() {
        assertThat(ByteArray.of((byte) 1, null, (byte) 3).values()).containsExactly((byte) 1,
            ByteArray.NULL_REPR, (byte) 3);
    }
}
