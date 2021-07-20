package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DoubleArrayTest {
    @Test
    void boxed() {
        assertThat(DoubleArray.of(1d, null, 3d)).containsExactly(1d, null, 3d);
    }

    @Test
    void raw() {
        assertThat(DoubleArray.of(1d, null, 3d).values()).containsExactly(1d, DoubleArray.NULL_REPR,
            3d);
    }
}
