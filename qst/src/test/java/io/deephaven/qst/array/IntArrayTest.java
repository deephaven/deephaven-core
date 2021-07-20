package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IntArrayTest {
    @Test
    void boxed() {
        assertThat(IntArray.of(1, null, 3)).containsExactly(1, null, 3);
    }

    @Test
    void raw() {
        assertThat(IntArray.of(1, null, 3).values()).containsExactly(1, IntArray.NULL_REPR, 3);
    }
}
