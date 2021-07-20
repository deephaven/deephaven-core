package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LongArrayTest {
    @Test
    void boxed() {
        assertThat(LongArray.of(1L, null, 3L)).containsExactly(1L, null, 3L);
    }

    @Test
    void raw() {
        assertThat(LongArray.of(1L, null, 3L).values()).containsExactly(1L, LongArray.NULL_REPR,
            3L);
    }
}
