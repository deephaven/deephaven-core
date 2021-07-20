package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ShortArrayTest {
    @Test
    void boxed() {
        assertThat(ShortArray.of((short) 1, null, (short) 3)).containsExactly((short) 1, null,
            (short) 3);
    }

    @Test
    void raw() {
        assertThat(ShortArray.of((short) 1, null, (short) 3).values()).containsExactly((short) 1,
            ShortArray.NULL_REPR, (short) 3);
    }
}
