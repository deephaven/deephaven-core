package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FloatArrayTest {

    @Test
    void raw() {
        assertThat(FloatArray.of(1f, null, 3f).values()).containsExactly(1f, FloatArray.NULL_REPR,
            3f);
    }
}
